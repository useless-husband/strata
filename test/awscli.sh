#!/usr/bin/env bash
# Drives the real AWS CLI v2 against a strata server on an ephemeral port.
#
# It runs twice: over plain HTTP, where the CLI signs a SHA-256 of every
# payload and sends a CRC64NVME checksum header, and over HTTPS (self-signed
# certificate made with openssl), where the CLI switches to aws-chunked
# bodies with unsigned payloads and a trailing CRC64NVME checksum.
#
# Usage: test/awscli.sh            (builds ./strata if STRATA_BIN is unset)
# Env:   STRATA_BIG_MB   size of the large upload in MiB (default 300)
#        STRATA_SCHEMES  "http https" (default) or one of them
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIG_MB=${STRATA_BIG_MB:-300}
SCHEMES=${STRATA_SCHEMES:-"http https"}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/strata-awscli.XXXXXX")
PID=""

cleanup() {
	if [[ -n "$PID" ]]; then
		kill "$PID" 2>/dev/null || true
		wait "$PID" 2>/dev/null || true
	fi
	rm -rf "$WORK"
}
trap cleanup EXIT

if [[ -z "${STRATA_BIN:-}" ]]; then
	(cd "$ROOT" && go build -o "$WORK/strata" ./cmd/strata)
	STRATA_BIN="$WORK/strata"
fi
command -v aws >/dev/null || { echo "aws CLI v2 not found"; exit 1; }

export AWS_ACCESS_KEY_ID=strata-itest
export AWS_SECRET_ACCESS_KEY=strata-itest-secret
export AWS_DEFAULT_REGION=us-east-1
export AWS_EC2_METADATA_DISABLED=true
export AWS_CONFIG_FILE=/dev/null
export AWS_SHARED_CREDENTIALS_FILE=/dev/null
unset AWS_PROFILE AWS_CA_BUNDLE || true

step() { printf '\n== %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; [[ -f "$WORK/server.log" ]] && tail -20 "$WORK/server.log" >&2; exit 1; }
expect_contains() { grep -qF -- "$2" <<<"$1" || fail "expected output to contain '$2', got:"$'\n'"$1"; }
expect_absent() { if grep -qF -- "$2" <<<"$1"; then fail "expected output not to contain '$2'"; fi; }

start_server() {
	local scheme=$1 tls=()
	rm -rf "$WORK/disks" "$WORK/addr"
	if [[ $scheme == https ]]; then
		openssl req -x509 -newkey rsa:2048 -nodes -keyout "$WORK/tls.key" -out "$WORK/tls.crt" \
			-days 1 -subj "/CN=127.0.0.1" -addext "subjectAltName=IP:127.0.0.1" >/dev/null 2>&1
		tls=(--tls-cert "$WORK/tls.crt" --tls-key "$WORK/tls.key")
	fi
	"$STRATA_BIN" server --address 127.0.0.1:0 --address-file "$WORK/addr" \
		--access-key "$AWS_ACCESS_KEY_ID" --secret-key "$AWS_SECRET_ACCESS_KEY" \
		--log-requests ${tls[@]+"${tls[@]}"} "$WORK"/disks/d{1..6} >"$WORK/server.log" 2>&1 &
	PID=$!
	for _ in $(seq 100); do [[ -s "$WORK/addr" ]] && break; sleep 0.1; done
	[[ -s "$WORK/addr" ]] || fail "server did not start"
	ENDPOINT="$scheme://$(cat "$WORK/addr")"
	CURL=(curl -sf)
	if [[ $scheme == https ]]; then
		export AWS_CA_BUNDLE="$WORK/tls.crt"
		CURL+=(--cacert "$WORK/tls.crt")
	else
		unset AWS_CA_BUNDLE
	fi
	echo "strata (pid $PID) at $ENDPOINT, 4 data + 2 parity disks"
}

stop_server() {
	kill "$PID"
	wait "$PID" 2>/dev/null || true
	PID=""
}

s3() { aws --endpoint-url "$ENDPOINT" s3 "$@"; }
s3api() { aws --endpoint-url "$ENDPOINT" s3api "$@"; }

# Test data, shared by both runs.
mkdir -p "$WORK/data/tree/sub/deeper"
echo "hello from strata" >"$WORK/data/small.txt"
head -c "$((BIG_MB * 1024 * 1024))" /dev/urandom >"$WORK/data/big.bin"
for i in 1 2 3; do head -c $((i * 70000)) /dev/urandom >"$WORK/data/tree/file$i.dat"; done
echo nested >"$WORK/data/tree/sub/nested.txt"
echo deepest >"$WORK/data/tree/sub/deeper/deep.txt"

run() {
	local scheme=$1
	start_server "$scheme"

	step "[$scheme] mb"
	s3 mb s3://itest
	out=$(s3 ls)
	expect_contains "$out" "itest"

	step "[$scheme] cp: small file, and a ${BIG_MB} MiB file (multipart)"
	s3 cp --only-show-errors "$WORK/data/small.txt" s3://itest/small.txt
	s3 cp --only-show-errors "$WORK/data/small.txt" "s3://itest/photos/2024/a b+c.txt"
	s3 cp --only-show-errors "$WORK/data/small.txt" s3://itest/photos/2025/x.txt
	start=$(date +%s)
	s3 cp --only-show-errors "$WORK/data/big.bin" s3://itest/big.bin
	echo "uploaded ${BIG_MB} MiB in $(($(date +%s) - start)) s"
	head=$(s3api head-object --bucket itest --key big.bin)
	expect_contains "$head" "\"ContentLength\": $((BIG_MB * 1024 * 1024))"
	etag=$(sed -n 's/.*"ETag": "\\"\([^\\]*\)\\"".*/\1/p' <<<"$head")
	[[ $etag == *-* ]] || fail "big.bin ETag $etag is not a multipart ETag"
	echo "big.bin ETag $etag (multipart, $(cut -d- -f2 <<<"$etag") parts)"
	grep -q 'api=UploadPart ' "$WORK/server.log" || fail "no UploadPart requests seen"
	if [[ $scheme == https ]]; then
		n=$(grep -c 'api=UploadPart .*status=200.*payload=aws-chunked-unsigned-trailer' "$WORK/server.log" || true)
		[[ $n -gt 0 ]] || fail "parts were not sent as aws-chunked with a trailer"
		echo "$n parts arrived as aws-chunked bodies with a trailing checksum"
	else
		n=$(grep -c 'api=UploadPart .*status=200.*payload=signed' "$WORK/server.log" || true)
		[[ $n -gt 0 ]] || fail "parts were not sent with signed payloads"
		echo "$n parts arrived with signed SHA-256 payloads"
	fi

	step "[$scheme] cp back and compare"
	s3 cp --only-show-errors s3://itest/big.bin "$WORK/big.down"
	cmp "$WORK/data/big.bin" "$WORK/big.down" || fail "big.bin differs after the round trip"
	rm "$WORK/big.down"
	got=$(s3 cp s3://itest/photos/2024/a\ b+c.txt -)
	[[ $got == "hello from strata" ]] || fail "small object came back as '$got'"
	echo "round trips are bit-exact"

	step "[$scheme] ls with prefixes"
	out=$(s3 ls s3://itest/)
	echo "$out"
	expect_contains "$out" "PRE photos/"
	expect_contains "$out" "small.txt"
	out=$(s3 ls s3://itest/photos/)
	expect_contains "$out" "PRE 2024/"
	expect_contains "$out" "PRE 2025/"
	out=$(s3 ls s3://itest/photos/2024/)
	expect_contains "$out" "a b+c.txt"
	expect_absent "$out" "x.txt"
	out=$(s3 ls --recursive s3://itest/photos)
	[[ $(wc -l <<<"$out") -eq 2 ]] || fail "recursive listing: $out"

	step "[$scheme] sync up, change, sync with --delete, sync down"
	cp -R "$WORK/data/tree" "$WORK/tree"
	s3 sync --only-show-errors "$WORK/tree" s3://itest/tree
	echo changed >"$WORK/tree/file2.dat"
	echo new >"$WORK/tree/sub/new.txt"
	rm "$WORK/tree/file3.dat"
	out=$(s3 sync --delete "$WORK/tree" s3://itest/tree)
	expect_contains "$out" "upload: "
	expect_contains "$out" "delete: s3://itest/tree/file3.dat"
	s3 sync --only-show-errors s3://itest/tree "$WORK/tree.down"
	diff -r "$WORK/tree" "$WORK/tree.down" || fail "synced tree differs"
	out=$(s3 sync "$WORK/tree" s3://itest/tree)
	[[ -z $out ]] || fail "second sync was not a no-op: $out"
	echo "sync round trip identical"

	step "[$scheme] presign + curl"
	url=$(s3 presign s3://itest/small.txt --expires-in 60)
	got=$("${CURL[@]}" "$url")
	[[ $got == "hello from strata" ]] || fail "presigned GET returned '$got'"
	code=$(curl -s -o /dev/null -w '%{http_code}' ${CURL[2]+"${CURL[@]:2}"} "${url%%X-Amz-Signature=*}X-Amz-Signature=0000")
	[[ $code == 403 ]] || fail "tampered presigned URL returned $code"
	echo "presigned URL works; a tampered one gets 403"

	step "[$scheme] s3api: metadata, copy, conditional and ranged get"
	s3api put-object --bucket itest --key meta.txt --body "$WORK/data/small.txt" \
		--content-type text/plain --metadata color=blue,owner=itest >/dev/null
	out=$(s3api head-object --bucket itest --key meta.txt)
	expect_contains "$out" '"color": "blue"'
	expect_contains "$out" '"ContentType": "text/plain"'
	s3api copy-object --bucket itest --key meta-copy.txt --copy-source itest/meta.txt >/dev/null
	out=$(s3api head-object --bucket itest --key meta-copy.txt)
	expect_contains "$out" '"owner": "itest"'
	s3api get-object --bucket itest --key small.txt --range bytes=6-9 "$WORK/range.out" >/dev/null
	[[ $(cat "$WORK/range.out") == "from" ]] || fail "ranged get returned '$(cat "$WORK/range.out")'"
	if s3api get-object --bucket itest --key small.txt --if-none-match "$(s3api head-object --bucket itest --key small.txt --query ETag --output text)" "$WORK/x" 2>"$WORK/err"; then
		fail "If-None-Match with the current ETag did not return 304"
	fi
	expect_contains "$(cat "$WORK/err")" "304"
	echo "metadata, copy, range and If-None-Match behave"

	step "[$scheme] errors"
	if s3 ls s3://no-such-bucket 2>"$WORK/err"; then fail "listing a missing bucket succeeded"; fi
	expect_contains "$(cat "$WORK/err")" "NoSuchBucket"
	if s3 rb s3://itest 2>"$WORK/err"; then fail "removing a non-empty bucket succeeded"; fi
	expect_contains "$(cat "$WORK/err")" "BucketNotEmpty"
	echo "NoSuchBucket and BucketNotEmpty reported"

	step "[$scheme] rm, rm --recursive, rb"
	s3 rm s3://itest/small.txt
	out=$(s3 ls s3://itest/)
	expect_absent "$out" "small.txt"
	s3 rm --recursive --only-show-errors s3://itest/
	[[ -z $(s3 ls --recursive s3://itest/) ]] || fail "bucket not empty after rm --recursive"
	s3 rb s3://itest
	s3 mb s3://forced
	s3 cp --only-show-errors "$WORK/data/small.txt" s3://forced/one
	s3 rb --force s3://forced >/dev/null
	out=$(s3 ls)
	expect_absent "$out" "itest"
	expect_absent "$out" "forced"

	if grep -E 'status=5[0-9][0-9]' "$WORK/server.log"; then fail "server returned 5xx"; fi
	echo "[$scheme] requests served: $(grep -c 'msg=request' "$WORK/server.log"), none failed with 5xx"
	stop_server
	rm -rf "$WORK/tree" "$WORK/tree.down"
}

for scheme in $SCHEMES; do
	run "$scheme"
done
echo
echo "PASS: aws CLI integration ($(aws --version 2>&1 | cut -d' ' -f1))"
