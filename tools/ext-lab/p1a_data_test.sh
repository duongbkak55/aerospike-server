#!/usr/bin/env bash
#
# P1a data-operation RBAC test with the official Java client.
#
# Usage: p1a_data_test.sh <ext-asd> <jar-with-aerospike-client>
#   e.g. the kvbench shaded jar (kvbench/bench-dist/target/kvbench.jar).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ASD="$1"
JAR="$2"

export EXT_LAB_DIR="${EXT_LAB_DIR:-$(mktemp -d)}"
export EXT_LAB_USER=admin EXT_LAB_PASSWORD=admin
CLUSTER="$SCRIPT_DIR/cluster.sh"
CLASSES="$EXT_LAB_DIR/classes"

mkdir -p "$EXT_LAB_DIR" "$CLASSES"
printf 'security {\n}\n' > "$EXT_LAB_DIR/security.conf"

javac -cp "$JAR" -d "$CLASSES" "$SCRIPT_DIR/java/DataRbacTest.java"

"$CLUSTER" start -n 2 -a "$ASD" -x "$EXT_LAB_DIR/security.conf" > /dev/null
trap '"$CLUSTER" stop' EXIT
"$CLUSTER" wait -n 2 > /dev/null

python3 - "$SCRIPT_DIR" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from asclient import Client
c = Client(port=3000)
c.login("admin", "admin")
c.create_user("rw", "rw-pw", ["read-write"])
c.create_user("ro", "ro-pw", ["read"])
c.create_user("wo", "wo-pw", ["write"])
c.create_role("s1-read-s2-rw", [("read", "test", "s1"),
                                ("read-write", "test", "s2")])
c.create_user("scoped", "scoped-pw", ["s1-read-s2-rw"])
PY

sleep 1
java -cp "$JAR:$CLASSES" DataRbacTest 3000
