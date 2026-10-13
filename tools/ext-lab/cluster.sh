#!/usr/bin/env bash
#
# Local multi-node cluster for ext feature tests - runs N asd processes on
# 127.0.0.1 with distinct ports (no Docker needed).
#
# Usage:
#   cluster.sh start [-n nodes] [-c cluster-index] [-a asd] [-x extra.conf]
#   cluster.sh stop  [-c cluster-index]
#   cluster.sh wait  [-n nodes] [-c cluster-index] [-t seconds]
#
# Node i (0-based) of cluster c listens on service port 3000 + 100*c + 10*i,
# fabric +1 and heartbeat +2. Two clusters (c=0, c=1) can run side by side for
# XDR tests. 'extra.conf' is appended verbatim at top level (e.g. a security
# or xdr context).
#
# Files go under $EXT_LAB_DIR (default: <repo>/run/ext-lab).

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

while getopts "n:c:a:x:t:" opt; do
	case "$opt" in
	n) nodes="$OPTARG" ;;
	c) cluster="$OPTARG" ;;
	a) asd="$OPTARG" ;;
	x) extra="$OPTARG" ;;
	t) timeout_s="$OPTARG" ;;
	*) exit 2 ;;
	esac
done

base_port() {
	echo $((3000 + 100 * cluster + 10 * $1))
}

node_dir() {
	echo "$LAB_DIR/c$cluster/n$1"
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

	storage-engine memory {
		data-size 1G
	}
}
EOF

	if [[ -n "$extra" ]]; then
		cat "$extra" >> "$dir/aerospike.conf"
	fi
}

start() {
	[[ -x "$asd" ]] || { echo "asd not found: $asd" >&2; exit 1; }

	for ((i = 0; i < nodes; i++)); do
		local dir
		dir="$(node_dir "$i")"
		mkdir -p "$dir/work/usr/udf/lua" "$dir/work/smd" "$dir/log"
		write_conf "$i"

		"$asd" --foreground --config-file "$dir/aerospike.conf" \
			> "$dir/log/console.log" 2>&1 &
		echo $! > "$dir/asd.pid"
		echo "node $i: pid $(cat "$dir/asd.pid") port $(base_port "$i")"
	done
}

stop() {
	shopt -s nullglob

	for pid_file in "$LAB_DIR/c$cluster"/n*/asd.pid; do
		local pid
		pid="$(cat "$pid_file")"

		if kill "$pid" 2>/dev/null; then
			for _ in $(seq 50); do
				kill -0 "$pid" 2>/dev/null || break
				sleep 0.2
			done

			kill -9 "$pid" 2>/dev/null || true
		fi

		rm -f "$pid_file"
	done
}

wait_cluster() {
	local deadline=$((SECONDS + timeout_s)) size

	while ((SECONDS < deadline)); do
		size="$("$SCRIPT_DIR/asinfo.py" -p "$(base_port 0)" \
			statistics 2>/dev/null | tr ';' '\n' |
			sed -n 's/^cluster_size=//p')" || true

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
*)
	sed -n '3,15p' "$0"
	exit 2
	;;
esac
