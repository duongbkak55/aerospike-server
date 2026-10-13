#!/usr/bin/env bash
#
# P3a XDR test - two local clusters (c0 on port 3000, c1 on port 3100), each
# shipping namespace 'test' to the other (active-active).
#
# Usage: p3a_xdr_test.sh <ext-asd> <jar-with-aerospike-client>
#   e.g. the kvbench shaded jar (kvbench/bench-dist/target/kvbench.jar).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ASD="$1"
JAR="$2"

export EXT_LAB_DIR="${EXT_LAB_DIR:-$(mktemp -d)}"
CLUSTER="$SCRIPT_DIR/cluster.sh"
CLASSES="$EXT_LAB_DIR/classes"
failures=0

mkdir -p "$CLASSES"
javac -cp "$JAR" -d "$CLASSES" "$SCRIPT_DIR/java/TombstoneTool.java" || exit 1

trap '"$CLUSTER" stop -c 0 > /dev/null; "$CLUSTER" stop -c 1 > /dev/null' EXIT

check() {
	if [[ "$1" == "$2" ]]; then
		echo "PASS: $3"
	else
		echo "FAIL: $3 (got '$1', want '$2')"
		failures=$((failures + 1))
	fi
}

# tool <port> <step> <args...> - prints the step's result number.
tool() {
	java -cp "$JAR:$CLASSES" TombstoneTool "$@" 2>/dev/null |
		sed -n 's/^RESULT //p'
}

# eventually <want> <port> <step> <args...> - polls a tool step until it
# returns <want> (up to 20s), then prints the last result.
eventually() {
	local want="$1" got=""
	shift

	for _ in $(seq 20); do
		got="$(tool "$@")"
		[[ "$got" == "$want" ]] && break
		sleep 1
	done

	echo "$got"
}

# info <port> <command> - with EXT_LAB_USER set, logs in first.
info() {
	if [[ -n "${EXT_LAB_USER:-}" ]]; then
		"$SCRIPT_DIR/asclient.py" -p "$1" -U "$EXT_LAB_USER" \
			-P "$EXT_LAB_PASSWORD" info "$2" | cut -f2-
	else
		"$SCRIPT_DIR/asinfo.py" -p "$1" "$2"
	fi
}

# xdr_stat <port> <dc> <name>
xdr_stat() {
	info "$1" "get-stats:context=xdr;dc=$2" | tr ';' '\n' |
		sed -n "s/^$3=//p"
}

# xdr_sum <port...> <dc> <name> - a DC statistic summed over nodes.
xdr_sum() {
	local dc="${*: -2:1}" name="${*: -1}" total=0

	for port in "${@:1:$#-2}"; do
		total=$((total + $(xdr_stat "$port" "$dc" "$name")))
	done

	echo "$total"
}

ns_stat() {
	info "$1" namespace/test | tr ';' '\n' | sed -n "s/^$2=//p"
}

wait_stable() {
	local n="$1" port="$2"

	for _ in $(seq 60); do
		if info "$port" "cluster-stable:size=$n;ignore-migrations=false" \
				2>/dev/null | grep -qv ERROR; then
			return 0
		fi

		sleep 1
	done

	echo "cluster at port $port not stable at size $n" >&2
	return 1
}

# Nodes that gain partitions while the cluster forms recover them - wait, so
# that recovery doesn't ship this test's writes a second time.
wait_recovered() {
	sleep 3

	for _ in $(seq 60); do
		local busy=0

		for spec in "$@"; do
			read -r port dc <<< "$spec"

			if [[ "$(xdr_stat "$port" "$dc" recovering)" != false ]]; then
				busy=1
			fi
		done

		[[ $busy == 0 ]] && return 0
		sleep 1
	done

	echo "recovery not done" >&2
	return 1
}

# xdr_conf <file> <dc> <port> [extra dc lines]
xdr_conf() {
	printf 'xdr {\n\tdc %s {\n\t\tnode-address-port 127.0.0.1 %s\n%s\t\tnamespace test {\n\t\t}\n\t}\n}\n' \
		"$2" "$3" "${4:-}" > "$1"
}

echo "== active-active"

xdr_conf "$EXT_LAB_DIR/x0.conf" DC2 3100
xdr_conf "$EXT_LAB_DIR/x1.conf" DC1 3000
"$CLUSTER" start -n 2 -c 0 -a "$ASD" -x "$EXT_LAB_DIR/x0.conf" > /dev/null
"$CLUSTER" start -n 2 -c 1 -a "$ASD" -x "$EXT_LAB_DIR/x1.conf" > /dev/null
wait_stable 2 3000 && wait_stable 2 3100 || exit 1
wait_recovered "3000 DC2" "3010 DC2" "3100 DC1" "3110 DC1" || exit 1

tool 3000 write a 200 1 > /dev/null
check "$(eventually 200 3100 count-value a 200 1)" 200 "writes ship c0 -> c1"

tool 3000 write a 200 2 > /dev/null
check "$(eventually 200 3100 count-value a 200 2)" 200 "updates ship c0 -> c1"

tool 3110 write b 100 7 > /dev/null
check "$(eventually 100 3000 count-value b 100 7)" 100 "writes ship c1 -> c0"

# A loop would keep bumping generations on both sides.
sleep 2
check "$(tool 3100 generation a 0)" 2 "no loop - generation on destination"
check "$(tool 3000 generation a 0)" 2 "no loop - generation on source"
check "$(xdr_sum 3000 3010 DC2 success)" 400 "c0 shipped each write once"
check "$(xdr_sum 3100 3110 DC1 filtered_out)" 400 "c1 filtered all writes from c0"

tool 3000 delete a 50 durable > /dev/null
check "$(eventually 150 3100 count a 200)" 150 "durable deletes ship"
check "$(($(ns_stat 3100 master_tombstones) + $(ns_stat 3110 master_tombstones)))" \
	50 "durable deletes leave tombstones on destination"
sleep 1
check "$(xdr_sum 3100 3110 DC1 success)" 100 "durable deletes from XDR aren't shipped back"

tool 3110 delete b 30 plain > /dev/null
check "$(eventually 70 3000 count b 100)" 70 "non-durable deletes ship"

tool 3000 write a 50 3 > /dev/null
check "$(eventually 50 3100 count-value a 50 3)" 50 "writes over tombstones ship"

check "$(xdr_sum 3000 3010 DC2 in_queue)" 0 "c0 queues drained"
check "$(xdr_sum 3000 3010 DC2 abandoned)" 0 "c0 abandoned nothing"
check "$(xdr_sum 3100 3110 DC1 abandoned)" 0 "c1 abandoned nothing"
check "$(info 3000 'get-config:context=xdr' | tr ';' '\n' | sed -n 's/^dcs=//p')" \
	DC2 "get-config lists DCs"

echo "== destination down"

"$CLUSTER" stop -c 1 > /dev/null
tool 3000 write d 40 5 > /dev/null
sleep 2
check "$(( $(xdr_sum 3000 3010 DC2 in_queue) > 0 ))" 1 "writes queue while destination is down"
"$CLUSTER" start -n 2 -c 1 -a "$ASD" -x "$EXT_LAB_DIR/x1.conf" > /dev/null
wait_stable 2 3100 || exit 1
check "$(eventually 40 3100 count-value d 40 5)" 40 "queued writes ship once destination is back"
check "$(( $(xdr_sum 3000 3010 DC2 retry_conn_reset) > 0 ))" 1 "connection retries counted"

echo "== xdr-write flag on file storage"

# Records written by XDR carry the flag on storage too - a cold restart must
# restore it, and a later local write must load the record and ship.
"$CLUSTER" stop -c 1 > /dev/null
rm -rf "$EXT_LAB_DIR/c1"
"$CLUSTER" start -n 2 -c 1 -a "$ASD" -s file -F 128M -x "$EXT_LAB_DIR/x1.conf" > /dev/null
wait_stable 2 3100 || exit 1
tool 3000 write f 50 1 > /dev/null
check "$(eventually 50 3100 count-value f 50 1)" 50 "writes ship to file storage"
"$CLUSTER" stop -c 1 > /dev/null
"$CLUSTER" start -n 2 -c 1 -a "$ASD" -s file -F 128M -x "$EXT_LAB_DIR/x1.conf" > /dev/null
wait_stable 2 3100 || exit 1
check "$(tool 3100 count-value f 50 1)" 50 "XDR-written records survive cold restart"
check "$(tool 3110 write f 50 2)" 50 "local writes over XDR-written records after cold restart"
check "$(eventually 50 3000 count-value f 50 2)" 50 "those local writes ship back"
check "$(cat "$EXT_LAB_DIR"/c1/n*/log/aerospike.log | grep -c 'bad xdr-write')" 0 \
	"no xdr-write mismatches on storage"

"$CLUSTER" stop -c 0 > /dev/null
"$CLUSTER" stop -c 1 > /dev/null

echo "== destination with security"

rm -rf "$EXT_LAB_DIR/c0" "$EXT_LAB_DIR/c1"
printf 'security {\n}\n' > "$EXT_LAB_DIR/security.conf"
"$CLUSTER" start -n 2 -c 1 -a "$ASD" -x "$EXT_LAB_DIR/security.conf" > /dev/null
EXT_LAB_USER=admin EXT_LAB_PASSWORD=admin wait_stable 2 3100 || exit 1

python3 - "$SCRIPT_DIR" <<'PYEOF'
import sys
sys.path.insert(0, sys.argv[1])
from asclient import Client
c = Client(port=3100)
c.login("admin", "admin")
c.create_user("xdr", "xdr-pw", ["read-write"])
PYEOF

printf 'xdr-pw\n' > "$EXT_LAB_DIR/xdr.pw"
xdr_conf "$EXT_LAB_DIR/x0.conf" DC2 3100 \
	"$(printf '\t\tauth-mode internal\n\t\tauth-user xdr\n\t\tauth-password-file %s\n' \
		"$EXT_LAB_DIR/xdr.pw")"$'\n'
"$CLUSTER" start -n 2 -c 0 -a "$ASD" -x "$EXT_LAB_DIR/x0.conf" > /dev/null
wait_stable 2 3000 || exit 1

tool 3000 write s 100 9 > /dev/null
check "$(EXT_LAB_USER=xdr EXT_LAB_PASSWORD=xdr-pw eventually 100 3100 count-value s 100 9)" \
	100 "writes ship to a destination with security"
check "$(xdr_sum 3000 3010 DC2 abandoned)" 0 "nothing abandoned with security"

echo "$failures failure(s)"
exit $((failures != 0))
