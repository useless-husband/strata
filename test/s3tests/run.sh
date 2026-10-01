#!/usr/bin/env bash
# Runs ceph/s3-tests (the Ceph RGW S3 conformance suite, boto3 based) at a
# pinned commit against strata on an ephemeral port, and summarises the
# result. Extra arguments go to pytest (for example -k 'list_objects').
#
#   test/s3tests/run.sh                          # all of test_s3.py
#   S3TESTS_WORK=/some/dir test/s3tests/run.sh -k multipart
#
# Needs git, python3 and network access for the first run.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
COMMIT=5522d1c351f75bc00ae0f64f742f3f095f5939d9
WORK=${S3TESTS_WORK:-$(mktemp -d "${TMPDIR:-/tmp}/strata-s3tests.XXXXXX")}
mkdir -p "$WORK"
PID=""
trap '[[ -n $PID ]] && kill $PID 2>/dev/null; wait 2>/dev/null || true' EXIT

if [[ ! -d $WORK/s3-tests ]]; then
	git clone -q https://github.com/ceph/s3-tests.git "$WORK/s3-tests"
fi
git -C "$WORK/s3-tests" checkout -q "$COMMIT"
if [[ ! -x $WORK/venv/bin/pytest ]]; then
	python3 -m venv "$WORK/venv"
	"$WORK/venv/bin/pip" install -q -r "$WORK/s3-tests/requirements.txt" pytest-timeout
fi
BIN=${STRATA_BIN:-$WORK/strata}
[[ -n ${STRATA_BIN:-} ]] || (cd "$ROOT" && go build -o "$BIN" ./cmd/strata)

# The suite's users. strata has no per-user ownership: every key has full
# access to every bucket. The owner ID strata reports for a key is
# hex(sha256("strata-owner:" + key)) and its display name is the key.
owner() { printf 'strata-owner:%s' "$1" | shasum -a 256 | cut -d' ' -f1; }
MAIN=s3tmainaccess ALT=s3taltaccess TENANT=s3ttenantaccess
cat >"$WORK/credentials" <<CRED
$MAIN s3tmainsecret0001
$ALT s3taltsecret00001
$TENANT s3ttenantsecret01
CRED
rm -rf "$WORK/disks" "$WORK/addr"
"$BIN" server --address 127.0.0.1:0 --address-file "$WORK/addr" --credentials-file "$WORK/credentials" \
	--sync none --data 4 --parity 2 "$WORK"/disks/d{1..6} 2>"$WORK/server.log" &
PID=$!
for _ in $(seq 100); do [[ -s $WORK/addr ]] && break; sleep 0.1; done
ADDR=$(cat "$WORK/addr")

user() { # section access secret
	cat <<SEC
[$1]
display_name = $2
user_id = $(owner "$2")
email = $2@example.com
access_key = $2
secret_key = $3
SEC
}
{
	cat <<CONF
[DEFAULT]
host = ${ADDR%:*}
port = ${ADDR##*:}
is_secure = False
ssl_verify = False

[fixtures]
bucket prefix = s3t-{random}-

CONF
	user "s3 main" "$MAIN" s3tmainsecret0001
	echo "api_name = us-east-1"
	echo
	user "s3 alt" "$ALT" s3taltsecret00001
	echo
	user "s3 tenant" "$TENANT" s3ttenantsecret01
	echo "tenant = testx"
	echo
	for s in "iam" "iam root" "iam alt root"; do user "$s" "$MAIN" s3tmainsecret0001; echo; done
} >"$WORK/s3tests.conf"

cd "$WORK/s3-tests"
set +e
S3TEST_CONF="$WORK/s3tests.conf" "$WORK/venv/bin/pytest" s3tests/functional/test_s3.py \
	-p no:cacheprovider -q --timeout 120 --junitxml="$WORK/junit.xml" "$@" >"$WORK/pytest.log" 2>&1
set -e
tail -3 "$WORK/pytest.log"
"$WORK/venv/bin/python" - "$WORK/junit.xml" <<'PY'
import sys, xml.etree.ElementTree as ET
root = ET.parse(sys.argv[1]).getroot()
suite = root if root.tag == "testsuite" else root.find("testsuite")
counts = {"passed": 0, "failed": 0, "error": 0, "skipped": 0}
for case in suite.iter("testcase"):
    kind = "passed"
    for child in case:
        if child.tag in ("failure", "error", "skipped"):
            kind = {"failure": "failed", "error": "error", "skipped": "skipped"}[child.tag]
    counts[kind] += 1
print("s3-tests summary:", ", ".join(f"{v} {k}" for k, v in counts.items()))
PY
echo "details: $WORK/pytest.log, $WORK/junit.xml, server log $WORK/server.log"
