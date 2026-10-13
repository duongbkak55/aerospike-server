#!/usr/bin/env bash
#
# P3c XDR recovery test - cluster c0 (2 nodes, file storage, port 3000) ships
# to cluster c1 (2 nodes, port 3100) through tcpproxy.py on port 3200.
# Stopping the proxy cuts the DCs apart; writes made meanwhile must still
# arrive after queue overflows, a full restart of c0 and a c0 node crash.
#
# Usage: p3c_recovery_test.sh <ext-asd> <jar-with-aerospike-client>

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

cleanup() {
	partition
	"$CLUSTER" stop -c 0 > /dev/null
	"$CLUSTER" stop -c 1 > /dev/null
}

trap cleanup EXIT

check() {
	if [[ "$1" == "$2" ]]; then
		echo "PASS: $3"
	else
		echo "FAIL: $3 (got '$1', want '$2')"
		failures=$((failures + 1))
	fi
}

tool() {
	java -cp "$JAR:$CLASSES" TombstoneTool "$@" 2>/dev/null |
		sed -n 's/^RESULT //p'
}

# eventually <want> <seconds> <port> <step> <args...>
eventually() {
	local want="$1" secs="$2" got=""
	shift 2

	for _ in $(seq "$secs"); do
		got="$(tool "$@")"
		[[ "$got" == "$want" ]] && break
		sleep 1
	done

	echo "$got"
}

# stat <port> <name> - a DC2 statistic on one node.
stat() {
	"$SCRIPT_DIR/asinfo.py" -p "$1" "get-stats:context=xdr;dc=DC2" 2>/dev/null |
		tr ';' '\n' | sed -n "s/^$2=//p"
}

# sum <name> <port...>
sum() {
	local name="$1" total=0 v
	shift

	for port in "$@"; do
		v="$(stat "$port" "$name")"
		total=$((total + ${v:-0}))
	done

	echo "$total"
}

wait_stable() {
	local n="$1" port="$2"

	for _ in $(seq 90); do
		if "$SCRIPT_DIR/asinfo.py" -p "$port" \
				"cluster-stable:size=$n;ignore-migrations=false" 2>/dev/null |
				grep -qv ERROR; then
			return 0
		fi

		sleep 1
	done

	echo "cluster at port $port not stable at size $n" >&2
	return 1
}

# Queues empty and no recovery due or running on the given nodes.
wait_drained() {
	for _ in $(seq 120); do
		local busy=0

		for port in "$@"; do
			if [[ "$(stat "$port" in_queue)" != 0 ||
					"$(stat "$port" recovering)" != false ]]; then
				busy=1
			fi
		done

		[[ $busy == 0 ]] && return 0
		sleep 1
	done

	echo "XDR not drained" >&2
	return 1
}

partition() {
	if [[ -f "$EXT_LAB_DIR/proxy.pid" ]]; then
		kill "$(cat "$EXT_LAB_DIR/proxy.pid")" 2>/dev/null
		rm -f "$EXT_LAB_DIR/proxy.pid"
		sleep 0.5
	fi
}

heal() {
	"$SCRIPT_DIR/tcpproxy.py" 3200 3100 > /dev/null 2>&1 &
	echo $! > "$EXT_LAB_DIR/proxy.pid"
	sleep 0.5
}

start_c0() {
	"$CLUSTER" start -n 2 -c 0 -a "$ASD" -s file -F 256M \
		-x "$EXT_LAB_DIR/x0.conf" > /dev/null
}

printf 'xdr {\n\tdc DC2 {\n\t\tnode-address-port 127.0.0.1 3200\n\t\tnamespace test {\n\t\t\ttransaction-queue-limit 1024\n\t\t}\n\t}\n}\n' \
	> "$EXT_LAB_DIR/x0.conf"

heal
"$CLUSTER" start -n 2 -c 1 -a "$ASD" > /dev/null
start_c0
wait_stable 2 3100 && wait_stable 2 3000 || exit 1

echo "== baseline"

tool 3000 write a 100 1 > /dev/null
check "$(eventually 100 30 3100 count-value a 100 1)" 100 "writes ship"
wait_drained 3000 3010
lag="$(stat 3000 lst_lag)"
check "$(( lag > 0 && lag < 15000 ))" 1 "last-ship-time follows writes (lag $lag ms)"

echo "== hot keys"

partition
tool 3000 repeat h 20 50 > /dev/null
check "$(( $(sum hot_keys 3000 3010) >= 900 ))" 1 "rewrites of queued keys are merged"
heal
check "$(eventually 20 30 3100 count-value h 20 50)" 20 "latest versions ship after merging"

echo "== queue overflow"

wait_drained 3000 3010
partition
tool 3000 write o 6000 1 > /dev/null
check "$(( $(sum queue_overflows 3000 3010) > 0 ))" 1 "queues overflowed"
heal
check "$(eventually 6000 120 3100 count-value o 6000 1)" 6000 \
	"overflowed writes recovered from storage"
check "$(( $(sum recoveries 3000 3010) > 0 ))" 1 "recovery ran"
wait_drained 3000 3010

echo "== restart of the source cluster"

partition
tool 3000 write r 500 2 > /dev/null
sleep 7 # the last-ship-time is published every 5 s
"$CLUSTER" stop -c 0 > /dev/null
start_c0
wait_stable 2 3000 || exit 1
heal
check "$(eventually 500 120 3100 count-value r 500 2)" 500 \
	"writes queued before a full restart ship after it"
wait_drained 3000 3010

echo "== crash of a source node"

partition
tool 3000 write k 500 3 > /dev/null
sleep 7
kill -9 "$(cat "$EXT_LAB_DIR/c0/n1/asd.pid")"
rm -f "$EXT_LAB_DIR/c0/n1/asd.pid"
wait_stable 1 3000 || exit 1
heal
check "$(eventually 500 120 3100 count-value k 500 3)" 500 \
	"writes queued on a crashed node ship from the new master"
wait_drained 3000

"$CLUSTER" start-node -i 1 -n 2 -c 0 -a "$ASD" -s file -F 256M \
	-x "$EXT_LAB_DIR/x0.conf" > /dev/null
wait_stable 2 3000 || exit 1
tool 3000 write z 100 4 > /dev/null
check "$(eventually 100 60 3100 count-value z 100 4)" 100 "shipping continues after the node rejoins"
wait_drained 3000 3010
check "$(sum abandoned 3000 3010)" 0 "nothing abandoned"

echo "$failures failure(s)"
exit $((failures != 0))
