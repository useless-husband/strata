#!/usr/bin/env bash
# A short demonstration: upload with the AWS CLI, wipe two of six disks,
# read everything back, and watch strata heal. The README transcript is
# the output of this script.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d "${TMPDIR:-/tmp}/strata-demo.XXXXXX")
PID=""
trap '[[ -n $PID ]] && kill $PID 2>/dev/null; wait 2>/dev/null; rm -rf "$WORK"' EXIT
BIN=${STRATA_BIN:-$WORK/strata}
[[ -x $BIN ]] || (cd "$ROOT" && go build -o "$BIN" ./cmd/strata)

export AWS_ACCESS_KEY_ID=demo AWS_SECRET_ACCESS_KEY=demo-secret-key AWS_DEFAULT_REGION=us-east-1
export AWS_EC2_METADATA_DISABLED=true AWS_CONFIG_FILE=/dev/null AWS_SHARED_CREDENTIALS_FILE=/dev/null
export STRATA_ACCESS_KEY=$AWS_ACCESS_KEY_ID STRATA_SECRET_KEY=$AWS_SECRET_ACCESS_KEY

show() { printf '\n$ %s\n' "$*"; "$@"; }

"$BIN" server --address 127.0.0.1:0 --address-file "$WORK/addr" --data 4 --parity 2 \
	"$WORK"/disk{1..6} 2>"$WORK/server.log" &
PID=$!
for _ in $(seq 100); do [[ -s $WORK/addr ]] && break; sleep 0.1; done
EP="http://$(cat "$WORK/addr")"
s3() { aws --endpoint-url "$EP" s3 "$@"; }
cd "$WORK"

head -c 64000000 /dev/urandom >video.bin
echo "hello, erasure coding" >note.txt
show s3 mb s3://demo
show s3 cp --only-show-errors video.bin s3://demo/media/video.bin
show s3 cp --only-show-errors note.txt s3://demo/note.txt
show s3 ls --recursive s3://demo

echo
echo "# Every object is split into stripes; each disk holds one shard of each stripe."
show du -sh disk1 disk2 disk3 disk4 disk5 disk6

echo
echo "# Lose two disks: delete their directories outright."
show rm -rf disk2 disk5

show s3 cp --only-show-errors s3://demo/media/video.bin video.back
show cmp video.bin video.back
echo "(identical)"
show s3 cp s3://demo/note.txt -

echo
echo "# The reads were reconstructed from parity and queued the objects for healing;"
echo "# the wiped disks were noticed, reformatted and refilled."
sleep 2
show curl -s "$EP/-/metrics" -o metrics.txt
grep -E '^strata_(degraded_stripes|missing_shards|healed_objects|disks_replaced)_total' metrics.txt
show du -sh disk2 disk5

echo
echo "# A deep scrub reads and verifies every block of every object."
show "$BIN" scrub --endpoint "$EP"

echo
echo "# Bit rot: flip one bit in the middle of a shard file."
f=$(find disk3/buckets -name 'part.*' -size +1M | head -1)
python3 - "$f" <<'PY'
import os, sys
p = sys.argv[1]
mid = os.path.getsize(p) // 2
with open(p, "r+b") as f:
    f.seek(mid)
    b = f.read(1)
    f.seek(mid)
    f.write(bytes([b[0] ^ 0x40]))
PY
echo "flipped one bit in ${f#"$WORK"/}"
show s3 cp --only-show-errors s3://demo/media/video.bin video.back2
show cmp video.bin video.back2
echo "(identical)"
sleep 1
show curl -s "$EP/-/metrics" -o metrics.txt
grep -E '^strata_(corrupt_blocks|healed_objects)_total' metrics.txt
echo "# If the flipped block was a data shard, the read caught it (CRC32C mismatch),"
echo "# served the stripe from parity and healed the object; a parity block is only"
echo "# read by a scrub, which finds and repairs it:"
show "$BIN" heal --deep --endpoint "$EP"
show "$BIN" scrub --endpoint "$EP"
