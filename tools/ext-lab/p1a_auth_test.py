#!/usr/bin/env python3
"""P1a auth tests - security protocol, RBAC on admin and info commands.

Usage: p1a_auth_test.py <ext-asd>

Starts a 2-node ext-build cluster with security enabled (via cluster.sh),
runs the checks, restarts the cluster to check persistence, and stops it.
Data-operation checks (read/write with the official client) live in the Java
test - see tools/ext-lab/java.
"""

import os
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from asclient import Client, SecurityError  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
PORTS = [3000, 3010]

ERR_USER = 60
ERR_USER_EXISTS = 61
ERR_PASSWORD = 62
ERR_CREDENTIAL = 65
ERR_ROLE = 70
ERR_ROLE_EXISTS = 71
ERR_NOT_AUTHENTICATED = 80
ERR_ROLE_VIOLATION = 81

SECURITY_CONF = """
security {
	log {
		report-authentication true
		report-user-admin true
		report-violation true
	}
}
"""

failures = 0


def check(cond, what):
    global failures
    print(("PASS: " if cond else "FAIL: ") + what)

    if not cond:
        failures += 1


def expect_error(code, what, fn, *args):
    try:
        fn(*args)
    except SecurityError as e:
        check(e.code == code, f"{what} (got {e.code}, want {code})")
        return

    check(False, f"{what} (no error, want {code})")


def cluster(cmd, asd=None, extra=None):
    args = [os.path.join(HERE, "cluster.sh"), cmd, "-n", "2"]

    if asd:
        args += ["-a", asd]

    if extra:
        args += ["-x", extra]

    subprocess.run(args, check=True, stdout=subprocess.DEVNULL)


def login(user, password, port=PORTS[0]):
    c = Client(port=port)
    c.login(user, password)
    return c


def info_error(c, cmd):
    """Returns the error code of an info response, or None. Auth errors come
    back as the whole line, other errors as the command's value."""
    key, value = next(iter(c.info(cmd).items()))

    for text in (key, value):
        if text.startswith("ERROR:"):
            return int(text.split(":")[1])

    return None


def main():
    asd = sys.argv[1]
    lab = os.environ.setdefault("EXT_LAB_DIR", tempfile.mkdtemp())
    conf = os.path.join(lab, "security.conf")

    os.makedirs(lab, exist_ok=True)

    with open(conf, "w") as f:
        f.write(SECURITY_CONF)

    cluster("start", asd, conf)

    try:
        os.environ["EXT_LAB_USER"] = "admin"
        os.environ["EXT_LAB_PASSWORD"] = "admin"
        subprocess.run([os.path.join(HERE, "cluster.sh"), "wait", "-n", "2"],
                       check=True, stdout=subprocess.DEVNULL)
        run_checks()

        # Persistence - users, roles and passwords survive a restart.
        cluster("stop")
        cluster("start", asd, conf)
        time.sleep(3)
        check_persistence()
    finally:
        cluster("stop")

    print(f"{failures} failure(s)")
    return 1 if failures else 0


def run_checks():
    # Unauthenticated connections get nothing.
    c = Client(port=PORTS[0])
    check(info_error(c, "build") == ERR_NOT_AUTHENTICATED,
          "unauthenticated info is rejected")
    c.close()

    # Login.
    expect_error(ERR_USER, "unknown user can't log in", login, "nobody", "x")
    expect_error(ERR_CREDENTIAL, "wrong password is rejected",
                 login, "admin", "wrong")

    admin = login("admin", "admin")
    check(admin.token is not None and admin.ttl == 86400,
          "default admin logs in and gets a token with session-ttl")
    check(info_error(admin, "build") is None, "authenticated info works")
    check(admin.query_users().get("admin") == ["user-admin"],
          "default admin has role user-admin")

    # Default admin has user-admin only - no sys-admin.
    check(info_error(admin, "set-config:context=service;proto-fd-idle-ms=70000")
          == ERR_ROLE_VIOLATION, "user-admin can't set-config")

    # Users & roles.
    admin.create_user("alice", "alice-pw", ["read-write"])
    admin.create_user("ops", "ops-pw", ["sys-admin"])
    admin.create_role("set-reader", [("read", "test", "s1")])
    admin.create_user("bob", "bob-pw", ["set-reader"])

    expect_error(ERR_USER_EXISTS, "duplicate user is rejected",
                 admin.create_user, "alice", "x", ["read"])
    expect_error(ERR_ROLE, "unknown role is rejected",
                 admin.create_user, "carol", "x", ["no-such-role"])
    expect_error(ERR_ROLE_EXISTS, "predefined role can't be re-created",
                 admin.create_role, "read", [("read", "", "")])

    time.sleep(0.5)  # SMD propagation to the other node

    users = admin.query_users()
    check(users.get("bob") == ["set-reader"] and "alice" in users,
          "query-users lists new users and roles")

    roles = admin.query_roles()
    check(roles.get("set-reader") == [("read", "test", "s1")] and
          roles.get("read-write") == [("read-write", "", "")],
          "query-roles lists predefined and user-defined roles")

    # RBAC on admin and info commands.
    alice = login("alice", "alice-pw")
    expect_error(ERR_ROLE_VIOLATION, "read-write user can't create users",
                 alice.create_user, "eve", "x", ["read"])
    check(info_error(alice, "set-config:context=service;proto-fd-idle-ms=70000")
          == ERR_ROLE_VIOLATION, "read-write user can't set-config")
    check(alice.query_users("alice") == {"alice": ["read-write"]},
          "a user can query itself")
    expect_error(ERR_ROLE_VIOLATION, "a user can't query others",
                 alice.query_users, "admin")

    ops = login("ops", "ops-pw")
    check(info_error(ops, "set-config:context=service;proto-fd-idle-ms=70000")
          is None, "sys-admin can set-config")

    # Session token works on a new connection and on the other node.
    for port in PORTS:
        c = Client(port=port)
        c.authenticate("alice", alice.token)
        check(info_error(c, "build") is None,
              f"token authenticates a new connection on port {port}")
        c.close()

    c = Client(port=PORTS[1])
    expect_error(ERR_CREDENTIAL, "forged token is rejected",
                 c.authenticate, "alice", bytes(len(alice.token)))
    expect_error(ERR_CREDENTIAL, "token is bound to its user",
                 c.authenticate, "bob", alice.token)
    c.close()

    # Changing a password revokes tokens and existing connections.
    old_token = alice.token
    expect_error(ERR_PASSWORD, "change-password needs the old password",
                 alice.change_password, "alice", "wrong", "alice-pw2")
    alice.change_password("alice", "alice-pw", "alice-pw2")
    time.sleep(0.5)

    c = Client(port=PORTS[1])
    expect_error(ERR_CREDENTIAL, "old token is revoked after password change",
                 c.authenticate, "alice", old_token)
    c.close()

    check(info_error(alice, "build") == ERR_NOT_AUTHENTICATED,
          "existing connection is de-authenticated after password change")
    expect_error(ERR_CREDENTIAL, "old password no longer works",
                 login, "alice", "alice-pw")
    check(login("alice", "alice-pw2", PORTS[1]).token is not None,
          "new password works on the other node")

    # Revoking a role takes effect on live connections.
    check(info_error(ops, "log-set:id=0;any=info") is None,
          "sys-admin can log-set")
    admin.revoke_roles("ops", ["sys-admin"])
    time.sleep(0.5)
    check(info_error(ops, "log-set:id=0;any=info") == ERR_ROLE_VIOLATION,
          "revoked role stops working on a live connection")

    # Drop user.
    admin.drop_user("bob")
    time.sleep(0.5)
    expect_error(ERR_USER, "dropped user can't log in", login, "bob", "bob-pw")

    # Admin changes its own password - must win over the default after restart.
    admin.change_password("admin", "admin", "admin-pw2")
    time.sleep(0.5)


def check_persistence():
    expect_error(ERR_CREDENTIAL, "default admin password does not come back",
                 login, "admin", "admin")

    admin = login("admin", "admin-pw2")
    users = admin.query_users()
    check("alice" in users and "bob" not in users and
          users.get("ops") == [],
          "users and role changes survive restart")
    check(admin.query_roles().get("set-reader") == [("read", "test", "s1")],
          "user-defined roles survive restart")
    check(login("alice", "alice-pw2", PORTS[1]).token is not None,
          "changed password survives restart")


if __name__ == "__main__":
    sys.exit(main())
