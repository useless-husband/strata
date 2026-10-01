VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
GO_TEST_FLAGS ?= -p 4

.PHONY: build test race lint bench bench-rs bench-server interop boto3 awscli crash fuzz demo clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o strata ./cmd/strata

test:
	go test $(GO_TEST_FLAGS) ./...

race:
	go test $(GO_TEST_FLAGS) -race -count=1 ./...

lint:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && echo "gofmt needed on the files above" && exit 1)
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@latest ./...

# Reed-Solomon throughput on one core, NEON and portable kernels.
bench-rs:
	go test -run XXX -bench . -cpu 1 -benchtime 2s ./internal/rs

# S3 PUT/GET throughput and latency against a local server (see docs/BENCHMARKS.md).
bench-server: build
	go run ./tools/s3bench -strata ./strata

bench: bench-rs bench-server

# Official AWS SDK for Go v2 and minio-go against a strata server.
interop: build
	cd test/interop && STRATA_BIN=$(CURDIR)/strata go test -count=1 -v ./...

# boto3 in a scratch virtual environment.
boto3: build
	test -d .venv || python3 -m venv .venv
	.venv/bin/pip install -q boto3
	STRATA_BIN=$(CURDIR)/strata .venv/bin/python test/boto3_test.py

# The real AWS CLI over HTTP and HTTPS.
awscli: build
	STRATA_BIN=$(CURDIR)/strata test/awscli.sh

# Kill the server during uploads and check nothing partial is ever visible.
crash: build
	STRATA_BIN=$(CURDIR)/strata go test -count=1 -run TestKill -v ./cmd/strata

fuzz:
	go test -run XXX -fuzz FuzzReconstruct -fuzztime 30s ./internal/rs
	go test -run XXX -fuzz FuzzChunkedReader -fuzztime 30s ./internal/sigv4

demo: build
	test/demo.sh

clean:
	rm -rf strata dist coverage.out .venv
