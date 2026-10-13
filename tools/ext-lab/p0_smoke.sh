#!/usr/bin/env bash
#
# P0 smoke test - checks config gating in the CE and ext builds and that an
# ext-build cluster forms.
#
# Usage: p0_smoke.sh <ce-asd> <ext-asd>
#   (build each with 'make' / 'make USE_EXT=1' and copy target/.../bin/asd
#   aside, since both builds write the same path)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CE_ASD="$1"
EXT_ASD="$2"

export EXT_LAB_DIR="${EXT_LAB_DIR:-$(mktemp -d)}"
CLUSTER="$SCRIPT_DIR/cluster.sh"
SECURITY_CONF="$EXT_LAB_DIR/security.conf"
SC_CONF="$EXT_LAB_DIR/sc.conf"
failures=0

mkdir -p "$EXT_LAB_DIR"
printf 'security {\n}\n' > "$SECURITY_CONF"
printf 'strong-consistency true\n' > "$SC_CONF"

check() {
	if [[ "$1" == 0 ]]; then
		echo "PASS: $2"
	else
		echo "FAIL: $2"
		failures=$((failures + 1))
	fi
}

# Start one node with an extra config context; expect it to exit with 'msg'.
expect_rejected() {
	local asd="$1" conf="$2" msg="$3" what="$4" opt="${5:--x}" pid status=0
	local dir="$EXT_LAB_DIR/c0/n0"

	"$CLUSTER" start -n 1 -a "$asd" "$opt" "$conf" > /dev/null
	pid="$(cat "$dir/asd.pid")"

	for _ in $(seq 50); do
		kill -0 "$pid" 2>/dev/null || break
		sleep 0.2
	done

	if kill -0 "$pid" 2>/dev/null; then
		status=1 # still running - config was accepted
	fi

	"$CLUSTER" stop > /dev/null
	grep -q "$msg" "$dir/log/console.log" "$dir/log/aerospike.log" \
		2>/dev/null || status=1
	check "$status" "$what"
	rm -rf "$EXT_LAB_DIR/c0"
}

expect_rejected "$CE_ASD" "$SECURITY_CONF" "'security' is enterprise-only" \
	"CE build rejects security context"
expect_rejected "$EXT_ASD" "$SC_CONF" "'strong-consistency' is enterprise-only" \
	"ext build rejects enterprise-only items it doesn't provide" -N

"$CLUSTER" start -n 3 -a "$EXT_ASD" > /dev/null
"$CLUSTER" wait -n 3 -t 30 > /dev/null
check $? "ext build forms a 3-node cluster"

grep -q "ext build - ready config items: security" \
	"$EXT_LAB_DIR/c0/n0/log/aerospike.log"
check $? "ext build logs its ready config items"

# Without a security context, login answers 'not configured' (52), which
# clients treat as "no login needed".
python3 - "$SCRIPT_DIR" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from asclient import Client, SecurityError
try:
    Client(port=3000).login("anyone", "x")
    sys.exit(1)
except SecurityError as e:
    sys.exit(0 if e.code == 52 else 1)
PY
check $? "ext build without security answers login with 'not configured'"

"$CLUSTER" stop > /dev/null

echo "$failures failure(s)"
exit $((failures != 0))
