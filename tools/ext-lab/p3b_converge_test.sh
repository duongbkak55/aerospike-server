#!/usr/bin/env bash
#
# P3b bin convergence test - two local clusters (c0 on port 3000, c1 on port
# 3100) with conflict-resolve-writes, shipping to each other with bin LUTs.
# Each DC ships through a TCP proxy - stopping the proxies partitions the DCs,
# so both sides take conflicting writes, then must converge once healed.
#
# Usage: p3b_converge_test.sh <ext-asd> <jar-with-aerospike-client> [memory|file]
#   BIN_POLICY=only-changed ships only bins changed since the last-ship-time.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ASD="$1"
JAR="$2"
STORAGE="${3:-memory}"

export EXT_LAB_DIR="${EXT_LAB_DIR:-$(mktemp -d)}"
CLUSTER="$SCRIPT_DIR/cluster.sh"
CLASSES="$EXT_LAB_DIR/classes"
failures=0

mkdir -p "$CLASSES"
javac -cp "$JAR" -d "$CLASSES" "$SCRIPT_DIR/java/ConvergeTool.java" || exit 1

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

# tool <port> <step> <args...> - prints the step's result.
tool() {
	java -cp "$JAR:$CLASSES" ConvergeTool "$@" 2>/dev/null |
		sed -n 's/^RESULT //p'
}

xdr_sum() {
	local dc="$1" name="$2" total=0
	shift 2

	for port in "$@"; do
		total=$((total + $("$SCRIPT_DIR/asinfo.py" -p "$port" \
			"get-stats:context=xdr;dc=$dc" | tr ';' '\n' |
			sed -n "s/^$name=//p")))
	done

	echo "$total"
}

# Wait until both DCs have shipped everything.
wait_drained() {
	for _ in $(seq 60); do
		if [[ $(xdr_sum DC2 in_queue 3000 3010) == 0 &&
				$(xdr_sum DC1 in_queue 3100 3110) == 0 ]]; then
			sleep 1 # last ships in flight
			return 0
		fi

		sleep 1
	done

	echo "queues not drained" >&2
	return 1
}

wait_stable() {
	local port="$1"

	for _ in $(seq 60); do
		if "$SCRIPT_DIR/asinfo.py" -p "$port" \
				"cluster-stable:size=2;ignore-migrations=false" 2>/dev/null |
				grep -qv ERROR; then
			return 0
		fi

		sleep 1
	done

	return 1
}

partition() {
	for f in "$EXT_LAB_DIR"/proxy*.pid; do
		[[ -f "$f" ]] && kill "$(cat "$f")" 2>/dev/null
		rm -f "$f"
	done

	sleep 0.5
}

heal() {
	"$SCRIPT_DIR/tcpproxy.py" 3200 3100 > /dev/null 2>&1 &
	echo $! > "$EXT_LAB_DIR/proxy0.pid"
	"$SCRIPT_DIR/tcpproxy.py" 3300 3000 > /dev/null 2>&1 &
	echo $! > "$EXT_LAB_DIR/proxy1.pid"
	sleep 0.5
}

# both <set> <key-ix> <bin> - the bin's value on c0 and c1.
both() {
	echo "$(tool 3000 get "$1" "$2" "$3")/$(tool 3100 get "$1" "$2" "$3")"
}

# c <port> <step...> - a write step, then a short pause so the next write in
# the scenario gets a later LUT.
c() {
	tool "$@" > /dev/null
	sleep 0.05
}

printf '\tconflict-resolve-writes true\n' > "$EXT_LAB_DIR/ns.conf"

for i in 0 1; do
	printf 'xdr {\n\tsrc-id %s\n\tdc %s {\n\t\tnode-address-port 127.0.0.1 %s\n\t\tnamespace test {\n\t\t\tship-bin-luts true\n\t\t\tbin-policy %s\n\t\t}\n\t}\n}\n' \
		"$((i + 1))" "$([[ $i == 0 ]] && echo DC2 || echo DC1)" \
		"$([[ $i == 0 ]] && echo 3200 || echo 3300)" "${BIN_POLICY:-all}" \
		> "$EXT_LAB_DIR/x$i.conf"
done

heal

for i in 0 1; do
	"$CLUSTER" start -n 2 -c "$i" -a "$ASD" -s "$STORAGE" -F 128M \
		-N "$EXT_LAB_DIR/ns.conf" -x "$EXT_LAB_DIR/x$i.conf" > /dev/null
done

wait_stable 3000 && wait_stable 3100 || exit 1

echo "== merge without partition ($STORAGE storage, bin-policy ${BIN_POLICY:-all})"

c 3000 put m 0 a 1
wait_drained
c 3100 put m 0 b 2
wait_drained
check "$(both m 0 a)" "1/1" "bin written on c0 kept after c1 writes another bin"
check "$(both m 0 b)" "2/2" "bin written on c1 shipped to c0"
c 3100 delete-bin m 0 a
wait_drained
check "$(both m 0 a)" "-/-" "bin delete ships"
c 3000 delete m 0
wait_drained
check "$(both m 0 b)" "!/!" "record delete ships"

echo "== conflicts during a partition"

# Keys both sides hold before the partition.
for k in 3 4 5 6; do
	c 3000 put p "$k" x 1
	c 3000 put p "$k" y 1
done

wait_drained
partition

c 3000 put p 0 x 1;      c 3100 put p 0 x 2       # same bin - later write wins
c 3100 put p 1 x 2;      c 3000 put p 1 x 1       # ... either direction
c 3000 put p 2 a 1;      c 3100 put p 2 b 1       # different bins - merge
c 3100 put p 3 x 2;      c 3000 delete-bin p 3 x  # bin delete after write
c 3000 delete-bin p 4 x; c 3100 put p 4 x 2       # write after bin delete
c 3100 put p 5 x 2;      c 3000 delete p 5        # record delete after write
c 3000 delete p 6;       c 3100 put p 6 z 3       # write after record delete

heal
wait_drained

check "$(both p 0 x)" "2/2" "same bin - later write on c1 wins"
check "$(both p 1 x)" "1/1" "same bin - later write on c0 wins"
check "$(both p 2 a)/$(both p 2 b)" "1/1/1/1" "different bins merge"
check "$(both p 3 x)/$(both p 3 y)" "-/-/1/1" "later bin delete wins"
check "$(both p 4 x)" "2/2" "later write beats bin delete"
check "$(both p 5 x)" "!/!" "later record delete wins"
check "$(both p 6 z)/$(both p 6 x)" "3/3/-/-" "later write beats record delete - deleted bins stay deleted"

echo "== random writes on both sides during a partition"

partition
# Not a bare 'wait' - the proxies are background jobs too.
tool 3000 random r 40 600 4 1 > "$EXT_LAB_DIR/r1.out" & p1=$!
tool 3100 random r 40 600 4 2 > "$EXT_LAB_DIR/r2.out" & p2=$!
wait $p1 $p2
check "$(cat "$EXT_LAB_DIR"/r[12].out | tr '\n' ' ')" "600 600 " "random writers finished"
heal
wait_drained

d0="$(tool 3000 digest r 40)"
d1="$(tool 3100 digest r 40)"
check "$d0" "$d1" "random conflicting writes converge ($d0)"

echo "== random concurrent writes without partition"

tool 3000 random s 30 800 3 3 > "$EXT_LAB_DIR/s1.out" & p1=$!
tool 3100 random s 30 800 3 4 > "$EXT_LAB_DIR/s2.out" & p2=$!
tool 3010 random s 30 800 3 5 > "$EXT_LAB_DIR/s3.out" & p3=$!
tool 3110 random s 30 800 3 6 > "$EXT_LAB_DIR/s4.out" & p4=$!
wait $p1 $p2 $p3 $p4
check "$(cat "$EXT_LAB_DIR"/s[1-4].out | tr '\n' ' ')" "800 800 800 800 " \
	"concurrent writers finished"
wait_drained

d0="$(tool 3000 digest s 30)"
d1="$(tool 3100 digest s 30)"
check "$d0" "$d1" "concurrent writes converge ($d0)"

check "$(xdr_sum DC2 abandoned 3000 3010)/$(xdr_sum DC1 abandoned 3100 3110)" \
	"0/0" "nothing abandoned"

if [[ "$STORAGE" == file ]]; then
	echo "== cold restart keeps bin metadata"

	before="$(tool 3000 digest r 40)"

	for i in 0 1; do
		"$CLUSTER" stop -c "$i" > /dev/null
		"$CLUSTER" start -n 2 -c "$i" -a "$ASD" -s file -F 128M \
			-N "$EXT_LAB_DIR/ns.conf" -x "$EXT_LAB_DIR/x$i.conf" > /dev/null
	done

	wait_stable 3000 && wait_stable 3100 || exit 1
	check "$(tool 3000 digest r 40)" "$before" "data unchanged after cold restart"
	check "$(tool 3000 digest r 40)" "$(tool 3100 digest r 40)" \
		"still converged after cold restart"

	partition
	c 3000 put p 3 x 5   # p-3 x is a bin tombstone on both sides
	c 3100 delete-bin p 2 a
	heal
	wait_drained
	check "$(both p 3 x)/$(both p 2 a)" "5/5/-/-" "conflicts resolve after cold restart"
fi

echo "$failures failure(s)"
exit $((failures != 0))
