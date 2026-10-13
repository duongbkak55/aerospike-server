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
failures=0

mkdir -p "$EXT_LAB_DIR"
printf 'security {\n}\n' > "$SECURITY_CONF"

check() {
	if [[ "$1" == 0 ]]; then
		echo "PASS: $2"
	else
		echo "FAIL: $2"
		failures=$((failures + 1))
	fi
}

# Start one node with a security context; expect it to exit with 'msg'.
expect_rejected() {
	local asd="$1" msg="$2" what="$3" pid status=0
	local dir="$EXT_LAB_DIR/c0/n0"

	"$CLUSTER" start -n 1 -a "$asd" -x "$SECURITY_CONF" > /dev/null
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

expect_rejected "$CE_ASD" "'security' is enterprise-only" \
	"CE build rejects security context"
expect_rejected "$EXT_ASD" "'security' is not yet implemented (P1)" \
	"ext build rejects not-yet-implemented security context"

"$CLUSTER" start -n 3 -a "$EXT_ASD" > /dev/null
"$CLUSTER" wait -n 3 -t 30 > /dev/null
check $? "ext build forms a 3-node cluster"

grep -q "ext build - ready config items: none" \
	"$EXT_LAB_DIR/c0/n0/log/aerospike.log"
check $? "ext build logs its ready config items"

"$CLUSTER" stop > /dev/null

echo "$failures failure(s)"
exit $((failures != 0))
