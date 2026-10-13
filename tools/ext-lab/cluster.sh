#!/usr/bin/env bash
#
# Local multi-node cluster for ext feature tests - runs N asd processes on
# 127.0.0.1 with distinct ports (no Docker needed).
#
# Usage:
#   cluster.sh start      [-n nodes] [-c cluster-index] [-a asd] [-x extra.conf]
#                         [-s memory|file]
#   cluster.sh stop       [-c cluster-index]
#   cluster.sh wait       [-n nodes] [-c cluster-index] [-t seconds]
#   cluster.sh start-node -i node [-n nodes] [-c ...] [-a ...] [-x ...] [-s ...]
#   cluster.sh stop-node  -i node [-c cluster-index]
#
# '-s file' stores namespace 'test' in a file (cold start reloads it) instead of
# memory only. Nodes restarted with start-node keep their data.
#
# Node i (0-based) of cluster c listens on service port 3000 + 100*c + 10*i,
# fabric +1 and heartbeat +2. Two clusters (c=0, c=1) can run side by side for
# XDR tests. 'extra.conf' is appended verbatim at top level (e.g. a security
# or xdr context).
#
# Files go under $EXT_LAB_DIR (default: <repo>/run/ext-lab). With security
# enabled, set EXT_LAB_USER / EXT_LAB_PASSWORD for 'wait'.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
LAB_DIR="${EXT_LAB_DIR:-$REPO_DIR/run/ext-lab}"

cmd="${1:-}"
shift || true

nodes=3
cluster=0
asd="$REPO_DIR/target/Linux-$(uname -m)/bin/asd"
extra=""
timeout_s=30
storage=memory
node=""

while getopts "n:c:a:x:t:s:i:" opt; do
	case "$opt" in
	n) nodes="$OPTARG" ;;
	c) cluster="$OPTARG" ;;
	a) asd="$OPTARG" ;;
	x) extra="$OPTARG" ;;
	t) timeout_s="$OPTARG" ;;
	s) storage="$OPTARG" ;;
	i) node="$OPTARG" ;;
	*) exit 2 ;;
	esac
done

base_port() {
	echo $((3000 + 100 * cluster + 10 * $1))
}

node_dir() {
	echo "$LAB_DIR/c$cluster/n$1"
}

storage_conf() {
	if [[ "$storage" == file ]]; then
		printf '\tstorage-engine device {\n\t\tfile %s/data/test.dat\n\t\tfilesize 256M\n\t\tflush-max-ms 10\n\t}' \
			"$1"
	else
		printf '\tstorage-engine memory {\n\t\tdata-size 1G\n\t}'
	fi
}

write_conf() {
	local i="$1" dir port seeds=""
	dir="$(node_dir "$i")"
	port="$(base_port "$i")"

	for ((j = 0; j < nodes; j++)); do
		if [[ $j -ne $i ]]; then
			seeds+="		mesh-seed-address-port 127.0.0.1 $(($(base_port "$j") + 2))"$'\n'
		fi
	done

	cat > "$dir/aerospike.conf" <<EOF
service {
	run-as-daemon false
	proto-fd-max 1024
	work-directory $dir/work
	cluster-name ext-lab-$cluster
	node-id a$cluster$i
}

logging {
	file $dir/log/aerospike.log {
		context any info
	}
}

mod-lua {
	user-path $dir/work/usr/udf/lua
}

network {
	service {
		address 127.0.0.1
		access-address 127.0.0.1
		port $port
	}

	heartbeat {
		mode mesh
		address 127.0.0.1
		port $((port + 2))
$seeds		interval 150
		timeout 10
	}

	fabric {
		address 127.0.0.1
		port $((port + 1))
	}
}

namespace test {
	replication-factor 2

$(storage_conf "$dir")
}
EOF

	if [[ -n "$extra" ]]; then
		cat "$extra" >> "$dir/aerospike.conf"
	fi
}

start_node() {
	local i="$1" dir
	dir="$(node_dir "$i")"
	mkdir -p "$dir/work/usr/udf/lua" "$dir/work/smd" "$dir/log" "$dir/data"
	write_conf "$i"

	"$asd" --foreground --config-file "$dir/aerospike.conf" \
		>> "$dir/log/console.log" 2>&1 &
	echo $! > "$dir/asd.pid"
	echo "node $i: pid $(cat "$dir/asd.pid") port $(base_port "$i")"
}

start() {
	[[ -x "$asd" ]] || { echo "asd not found: $asd" >&2; exit 1; }

	for ((i = 0; i < nodes; i++)); do
		start_node "$i"
	done
}

# Graceful stop (SIGTERM, which flushes storage), SIGKILL after 10s.
stop_pid_file() {
	local pid_file="$1" pid
	pid="$(cat "$pid_file")"

	if kill "$pid" 2>/dev/null; then
		for _ in $(seq 50); do
			kill -0 "$pid" 2>/dev/null || break
			sleep 0.2
		done

		kill -9 "$pid" 2>/dev/null || true
	fi

	rm -f "$pid_file"
}

stop() {
	shopt -s nullglob

	for pid_file in "$LAB_DIR/c$cluster"/n*/asd.pid; do
		stop_pid_file "$pid_file"
	done
}

wait_cluster() {
	local deadline=$((SECONDS + timeout_s)) size

	while ((SECONDS < deadline)); do
		if [[ -n "${EXT_LAB_USER:-}" ]]; then
			size="$("$SCRIPT_DIR/asclient.py" -p "$(base_port 0)" \
				-U "$EXT_LAB_USER" -P "${EXT_LAB_PASSWORD:-}" info statistics \
				2>/dev/null | tr ';\t' '\n\n' |
				sed -n 's/^cluster_size=//p')" || true
		else
			size="$("$SCRIPT_DIR/asinfo.py" -p "$(base_port 0)" \
				statistics 2>/dev/null | tr ';' '\n' |
				sed -n 's/^cluster_size=//p')" || true
		fi

		if [[ "$size" == "$nodes" ]]; then
			echo "cluster $cluster: size $size"
			return 0
		fi

		sleep 1
	done

	echo "cluster $cluster: size '${size:-}' after ${timeout_s}s, want $nodes" >&2
	return 1
}

case "$cmd" in
start) start ;;
stop) stop ;;
wait) wait_cluster ;;
start-node) start_node "${node:?-i required}" ;;
stop-node)
	pid_file="$(node_dir "${node:?-i required}")/asd.pid"
	if [[ -f "$pid_file" ]]; then
		stop_pid_file "$pid_file"
	fi
	;;
*)
	sed -n '3,22p' "$0"
	exit 2
	;;
esac
