#!/usr/bin/env bash
#
# P2a tombstone (durable delete) tests with the official Java client.
#
# Usage: p2a_tombstone_test.sh <ext-asd> <jar-with-aerospike-client>
#   e.g. the kvbench shaded jar (kvbench/bench-dist/target/kvbench.jar).
#
# Durable-deleted records must stay deleted across cold restarts and when a
# node holding an older copy rejoins. Plain deletes are run alongside to show
# the CE "zombie" behavior the tombstones prevent - those are reported, not
# asserted.

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

trap '"$CLUSTER" stop > /dev/null' EXIT

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

# stat <port> <name> - a namespace 'test' statistic.
stat() {
	"$SCRIPT_DIR/asinfo.py" -p "$1" namespace/test | tr ';' '\n' |
		sed -n "s/^$2=//p"
}

# set_stat <port> <set> <name> - a set statistic in namespace 'test'.
set_stat() {
	"$SCRIPT_DIR/asinfo.py" -p "$1" "sets/test/$2" | tr ':;' '\n\n' |
		sed -n "s/^$3=//p"
}

# Wait for a stable cluster of <n> nodes with migrations done.
wait_stable() {
	local n="$1" port="${2:-3000}"

	for _ in $(seq 60); do
		if "$SCRIPT_DIR/asinfo.py" -p "$port" \
				"cluster-stable:size=$n;ignore-migrations=false" 2>/dev/null |
				grep -qv ERROR; then
			return 0
		fi

		sleep 1
	done

	echo "cluster not stable at size $n" >&2
	return 1
}

restart_node() {
	"$CLUSTER" stop-node -i "$1"
	"$CLUSTER" start-node -i "$1" -n "$2" -s file -a "$ASD" > /dev/null
}

#--------------------------------------------------------
echo "== single node, file storage, cold restart"

rm -rf "$EXT_LAB_DIR/c0"
"$CLUSTER" start -n 1 -s file -a "$ASD" > /dev/null
wait_stable 1

for set in z n; do
	tool 3000 write "$set" 20 1 > /dev/null
	tool 3000 write "$set" 20 2 > /dev/null # older version stays on disk
done

check "$(tool 3000 delete z 20 durable)" 20 "durable delete finds all records"
check "$(tool 3000 delete n 20 plain)" 20 "plain delete finds all records"
check "$(tool 3000 count z 20)" 0 "durable-deleted records read as not found"
check "$(tool 3000 scan z)" 0 "scan skips tombstones"
check "$(tool 3000 delete z 20 durable)" 0 "deleting a tombstone is not found"
check "$(stat 3000 tombstones)/$(stat 3000 objects)" 20/0 \
		"tombstones counted, not objects"

restart_node 0 1
wait_stable 1

check "$(tool 3000 count z 20)" 0 \
		"durable-deleted records stay deleted after cold restart"
check "$(stat 3000 tombstones)" 20 "tombstones are reloaded by cold start"
echo "INFO: plain-deleted records back after cold restart: $(tool 3000 count n 20)/20 (CE zombie behavior)"

check "$(tool 3000 generation z 0)" -1 "tombstone has no readable metadata"
tool 3000 write z 20 3 > /dev/null
check "$(tool 3000 count z 20)" 20 "write over a tombstone revives the record"
check "$(tool 3000 generation z 0)" 4 "revived record continues the generation"
check "$(stat 3000 tombstones)" 0 "revived records are no longer tombstones"

tool 3000 write b 10 1 > /dev/null
check "$(tool 3000 batch-delete b 10 durable)" 10 "batch durable delete"
check "$(tool 3000 count b 10)" 0 "batch durable-deleted records read as not found"
check "$(stat 3000 tombstones)" 10 "batch durable delete leaves tombstones"

restart_node 0 1
wait_stable 1
check "$(tool 3000 count b 10)" 0 \
		"batch durable-deleted records stay deleted after cold restart"
check "$(tool 3000 count z 20)" 20 "revived records survive cold restart"

tool 3000 write s 30 1 > /dev/null
tool 3000 delete s 10 durable > /dev/null
tool 3000 write s 5 2 > /dev/null
check "$(set_stat 3000 s objects)/$(set_stat 3000 s tombstones)" 25/5 \
		"set counts objects and tombstones"
restart_node 0 1
wait_stable 1
check "$(set_stat 3000 s objects)/$(set_stat 3000 s tombstones)" 25/5 \
		"set counts survive cold restart"
"$SCRIPT_DIR/asinfo.py" -p 3000 "truncate:namespace=test;set=s" > /dev/null
sleep 2
check "$(set_stat 3000 s objects)/$(set_stat 3000 s tombstones)" 0/0 \
		"truncate removes tombstones too"

"$CLUSTER" stop > /dev/null

#--------------------------------------------------------
echo "== two nodes, RF 2, file storage, node rejoin"

rm -rf "$EXT_LAB_DIR/c0"
"$CLUSTER" start -n 2 -s file -a "$ASD" > /dev/null
wait_stable 2

tool 3000 write r 50 1 > /dev/null
tool 3000 write p 50 1 > /dev/null

# Replicas get tombstones too.
check "$(tool 3000 delete r 10 durable)" 10 "durable delete on a 2-node cluster"
sleep 1
total=$(($(stat 3000 tombstones) + $(stat 3010 tombstones)))
check "$total" 20 "tombstones exist on master and replica (10 x RF 2)"

# Node 1 leaves holding live copies; deletes happen without it.
"$CLUSTER" stop-node -i 1
wait_stable 1
check "$(tool 3000 delete r 50 durable)" 40 "durable deletes while a node is away"
tool 3000 delete p 50 plain > /dev/null

"$CLUSTER" start-node -i 1 -n 2 -s file -a "$ASD" > /dev/null
wait_stable 2

check "$(tool 3000 count r 50)" 0 \
		"durable-deleted records stay deleted when a stale node rejoins"
echo "INFO: plain-deleted records back after the rejoin: $(tool 3000 count p 50)/50 (CE zombie behavior)"

# The surviving copies are tombstones - lose node 0 and read from node 1.
"$CLUSTER" stop-node -i 0
wait_stable 1 3010
check "$(tool 3010 count r 50)" 0 "replica copies are tombstones too"

"$CLUSTER" stop > /dev/null

#--------------------------------------------------------
echo "== memory storage"

rm -rf "$EXT_LAB_DIR/c0"
"$CLUSTER" start -n 2 -a "$ASD" > /dev/null
wait_stable 2

tool 3000 write m 30 1 > /dev/null
check "$(tool 3000 delete m 30 durable)" 30 "durable delete with memory storage"
check "$(tool 3000 count m 30)" 0 "memory tombstones read as not found"
sleep 1
total=$(($(stat 3000 tombstones) + $(stat 3010 tombstones)))
check "$total" 60 "memory tombstones on master and replica"

echo "$failures failure(s)"
exit $((failures != 0))
