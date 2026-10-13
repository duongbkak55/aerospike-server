#!/usr/bin/env bash
#
# P2b tomb raider tests with the official Java client.
#
# Usage: p2b_tomb_raider_test.sh <ext-asd> <jar-with-aerospike-client>
#   e.g. the kvbench shaded jar (kvbench/bench-dist/target/kvbench.jar).
#
# Runs with a short tomb-raider-period and eligible-age. Tombstones must be
# removed once eligible - but with file storage only after every older copy of
# their record is overwritten, so a cold restart can never resurrect one.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ASD="$1"
JAR="$2"

export EXT_LAB_DIR="${EXT_LAB_DIR:-$(mktemp -d)}"
CLUSTER="$SCRIPT_DIR/cluster.sh"
CLASSES="$EXT_LAB_DIR/classes"
NS_CONF="$EXT_LAB_DIR/ns.conf"
failures=0

mkdir -p "$CLASSES"
javac -cp "$JAR" -d "$CLASSES" "$SCRIPT_DIR/java/TombstoneTool.java" || exit 1

printf '\ttomb-raider-period 2\n\ttomb-raider-eligible-age 3\n' > "$NS_CONF"

trap '"$CLUSTER" stop > /dev/null' EXIT

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

stat() {
	"$SCRIPT_DIR/asinfo.py" -p "$1" namespace/test | tr ';' '\n' |
		sed -n "s/^$2=//p"
}

wait_stable() {
	for _ in $(seq 60); do
		if "$SCRIPT_DIR/asinfo.py" -p 3000 \
				"cluster-stable:size=$1;ignore-migrations=false" 2>/dev/null |
				grep -qv ERROR; then
			return 0
		fi

		sleep 1
	done

	echo "cluster not stable at size $1" >&2
	return 1
}

# wait_tombstones <want> <seconds> - poll until the tombstone count matches.
wait_tombstones() {
	local n=""

	for _ in $(seq "$2"); do
		n="$(stat 3000 tombstones)"

		if [[ "$n" == "$1" ]]; then
			break
		fi

		sleep 1
	done

	echo "$n"
}

start_one() {
	"$CLUSTER" start -n 1 -a "$ASD" -N "$NS_CONF" "$@" > /dev/null
	wait_stable 1
}

#--------------------------------------------------------
echo "== memory storage (not persisted)"

rm -rf "$EXT_LAB_DIR/c0"
start_one

tool 3000 write t 20 1 > /dev/null
tool 3000 delete t 20 durable > /dev/null
check "$(stat 3000 tombstones)" 20 "durable deletes leave tombstones"

# Revived before becoming eligible - must survive the raider.
tool 3000 write u 10 1 > /dev/null
tool 3000 delete u 10 durable > /dev/null
tool 3000 write u 10 2 > /dev/null

check "$(wait_tombstones 0 20)" 0 "tomb raider removes eligible tombstones"
check "$(tool 3000 count t 20)" 0 "removed tombstones' records stay deleted"
check "$(tool 3000 count u 10)" 10 "revived records are not removed"
check "$(stat 3000 objects)" 10 "object count unaffected by removal"
grep -q "tomb raider: .* removed" "$EXT_LAB_DIR/c0/n0/log/aerospike.log"
check $? 0 "tomb raider logs its cycles"

"$CLUSTER" stop > /dev/null

#--------------------------------------------------------
echo "== file storage - older copies on storage"

rm -rf "$EXT_LAB_DIR/c0"
start_one -s file -F 256M

tool 3000 write t 20 1 > /dev/null
tool 3000 write t 20 2 > /dev/null
tool 3000 delete t 20 durable > /dev/null

sleep 9 # eligible, and a few raider cycles
check "$(stat 3000 tombstones)" 20 \
		"tombstones kept while older copies remain on storage"
grep -q "tomb raider: 20 tombstones, 20 eligible, 0 removed" \
		"$EXT_LAB_DIR/c0/n0/log/aerospike.log"
check $? 0 "storage scan unmarks tombstones with older copies"

# Cycle the file through defrag so the older copies are overwritten.
tool 3000 churn c 10 600 100000 > /dev/null

check "$(wait_tombstones 0 60)" 0 \
		"tombstones removed once older copies are overwritten"
check "$(tool 3000 count t 20)" 0 "records stay deleted after removal"

"$CLUSTER" stop-node -i 0
"$CLUSTER" start-node -i 0 -n 1 -s file -F 256M -a "$ASD" -N "$NS_CONF" \
		> /dev/null
wait_stable 1

check "$(tool 3000 count t 20)" 0 \
		"no resurrection after cold restart following removal"
check "$(stat 3000 tombstones)" 0 \
		"cold start drops reloaded tombstones with no older copies"
check "$(tool 3000 count c 10)" 10 "other records survive cold restart"

echo "$failures failure(s)"
exit $((failures != 0))
