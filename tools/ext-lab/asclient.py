#!/usr/bin/env python3
"""Minimal security-protocol client for ext-lab tests.

Speaks the same wire format as the official clients: login, authenticate,
user/role administration, and info commands on an authenticated socket.
Passwords are bcrypt-hashed with the clients' fixed salt (via libcrypt).

Library use:  c = Client(port=3000); c.login("admin", "admin"); c.info("build")
CLI use:      asclient.py -p 3000 -U admin -P admin info build
              asclient.py -U admin -P admin create-user alice pw read-write
              asclient.py -U admin -P admin query-users
"""

import argparse
import ctypes
import ctypes.util
import socket
import struct
import sys

PROTO_VERSION = 2
TYPE_INFO = 1
TYPE_SECURITY = 2

CMD = {
    "authenticate": 0, "create_user": 1, "drop_user": 2, "set_password": 3,
    "change_password": 4, "grant_roles": 5, "revoke_roles": 6,
    "query_users": 9, "create_role": 10, "drop_role": 11,
    "grant_privileges": 12, "revoke_privileges": 13, "query_roles": 16,
    "login": 20,
}

F_USER, F_PASSWORD, F_OLD_PASSWORD, F_CREDENTIAL = 0, 1, 2, 3
F_SESSION_TOKEN, F_SESSION_TTL = 5, 6
F_ROLES, F_ROLE, F_PRIVILEGES = 10, 11, 12

PRIV = {
    "user-admin": 0, "sys-admin": 1, "data-admin": 2, "udf-admin": 3,
    "sindex-admin": 4, "read": 10, "read-write": 11, "read-write-udf": 12,
    "write": 13, "truncate": 14,
}
PRIV_NAME = {v: k for k, v in PRIV.items()}

QUERY_END = 50

_libcrypt = ctypes.CDLL(ctypes.util.find_library("crypt"))
_libcrypt.crypt.restype = ctypes.c_char_p


def credential(password):
    """bcrypt hash with the official clients' fixed salt."""
    return _libcrypt.crypt(password.encode(), b"$2a$10$7EqJtq98hPqEX7fNZaFWoO")


class SecurityError(Exception):
    def __init__(self, code, command):
        super().__init__(f"{command}: result {code}")
        self.code = code


class Client:
    def __init__(self, host="127.0.0.1", port=3000, timeout=10.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.user = None
        self.token = None
        self.ttl = None

    def close(self):
        self.sock.close()

    # -- transport --

    def _send(self, ptype, body):
        hdr = struct.pack(">Q", (PROTO_VERSION << 56) | (ptype << 48)
                          | len(body))
        self.sock.sendall(hdr + body)

    def _recv(self):
        hdr = self._recv_all(8)
        size = struct.unpack(">Q", hdr)[0] & 0xFFFFFFFFFFFF
        return self._recv_all(size)

    def _recv_all(self, n):
        buf = b""

        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))

            if not chunk:
                raise ConnectionError("connection closed")

            buf += chunk

        return buf

    # -- security messages --

    @staticmethod
    def _field(fid, data):
        return struct.pack(">IB", len(data) + 1, fid) + data

    def _sec(self, command, fields):
        body = bytes([0, 0, CMD[command], len(fields)]) + bytes(12)
        body += b"".join(self._field(fid, data) for fid, data in fields)
        self._send(TYPE_SECURITY, body)
        return self._recv()

    @staticmethod
    def _parse_msgs(buf):
        """Yield (result, {field_id: data}) per as_sec_msg in a response."""
        at = 0

        while at < len(buf):
            result, n_fields = buf[at + 1], buf[at + 3]
            at += 16
            fields = {}

            for _ in range(n_fields):
                size, fid = struct.unpack_from(">IB", buf, at)
                fields[fid] = buf[at + 5:at + 4 + size]
                at += 4 + size

            yield result, fields

    def _simple(self, command, fields):
        result, _ = next(self._parse_msgs(self._sec(command, fields)))

        if result != 0:
            raise SecurityError(result, command)

    @staticmethod
    def _roles(roles):
        out = bytes([len(roles)])

        for r in roles:
            out += bytes([len(r)]) + r.encode()

        return out

    @staticmethod
    def _privs(privs):
        """privs: list of (name, ns, set) - ns/set '' for global."""
        out = bytes([len(privs)])

        for name, ns, set_name in privs:
            out += bytes([PRIV[name]])

            if PRIV[name] >= 10:
                out += bytes([len(ns)]) + ns.encode()
                out += bytes([len(set_name)]) + set_name.encode()

        return out

    # -- commands --

    def login(self, user, password):
        resp = self._sec("login", [(F_USER, user.encode()),
                                   (F_CREDENTIAL, credential(password))])
        result, fields = next(self._parse_msgs(resp))

        if result != 0:
            raise SecurityError(result, "login")

        self.user = user
        self.token = fields.get(F_SESSION_TOKEN)
        self.ttl = struct.unpack(">I", fields[F_SESSION_TTL])[0] \
            if F_SESSION_TTL in fields else None
        return self.token

    def authenticate(self, user, token):
        self._simple("authenticate", [(F_USER, user.encode()),
                                      (F_SESSION_TOKEN, token)])

    def create_user(self, user, password, roles):
        self._simple("create_user", [(F_USER, user.encode()),
                                     (F_PASSWORD, credential(password)),
                                     (F_ROLES, self._roles(roles))])

    def drop_user(self, user):
        self._simple("drop_user", [(F_USER, user.encode())])

    def set_password(self, user, password):
        self._simple("set_password", [(F_USER, user.encode()),
                                      (F_PASSWORD, credential(password))])

    def change_password(self, user, old, new):
        self._simple("change_password", [(F_USER, user.encode()),
                                         (F_OLD_PASSWORD, credential(old)),
                                         (F_PASSWORD, credential(new))])

    def grant_roles(self, user, roles):
        self._simple("grant_roles", [(F_USER, user.encode()),
                                     (F_ROLES, self._roles(roles))])

    def revoke_roles(self, user, roles):
        self._simple("revoke_roles", [(F_USER, user.encode()),
                                      (F_ROLES, self._roles(roles))])

    def create_role(self, role, privs):
        self._simple("create_role", [(F_ROLE, role.encode()),
                                     (F_PRIVILEGES, self._privs(privs))])

    def drop_role(self, role):
        self._simple("drop_role", [(F_ROLE, role.encode())])

    def grant_privileges(self, role, privs):
        self._simple("grant_privileges", [(F_ROLE, role.encode()),
                                          (F_PRIVILEGES, self._privs(privs))])

    def revoke_privileges(self, role, privs):
        self._simple("revoke_privileges", [(F_ROLE, role.encode()),
                                           (F_PRIVILEGES, self._privs(privs))])

    def query_users(self, user=None):
        fields = [(F_USER, user.encode())] if user else []
        users = {}

        for result, f in self._parse_msgs(self._sec("query_users", fields)):
            if result == QUERY_END:
                break

            if result != 0:
                raise SecurityError(result, "query_users")

            users[f[F_USER].decode()] = self._decode_roles(f.get(F_ROLES, b"\0"))

        return users

    def query_roles(self, role=None):
        fields = [(F_ROLE, role.encode())] if role else []
        roles = {}

        for result, f in self._parse_msgs(self._sec("query_roles", fields)):
            if result == QUERY_END:
                break

            if result != 0:
                raise SecurityError(result, "query_roles")

            roles[f[F_ROLE].decode()] = self._decode_privs(
                f.get(F_PRIVILEGES, b"\0"))

        return roles

    @staticmethod
    def _decode_roles(data):
        out, at = [], 1

        for _ in range(data[0]):
            n = data[at]
            out.append(data[at + 1:at + 1 + n].decode())
            at += 1 + n

        return out

    @staticmethod
    def _decode_privs(data):
        out, at = [], 1

        for _ in range(data[0]):
            code = data[at]
            at += 1
            ns = set_name = ""

            if code >= 10:
                n = data[at]
                ns = data[at + 1:at + 1 + n].decode()
                at += 1 + n
                n = data[at]
                set_name = data[at + 1:at + 1 + n].decode()
                at += 1 + n

            out.append((PRIV_NAME.get(code, str(code)), ns, set_name))

        return out

    # -- info --

    def info(self, *commands):
        self._send(TYPE_INFO, "".join(c + "\n" for c in commands).encode())
        data = self._recv().decode(errors="replace")
        result = {}

        for line in data.split("\n"):
            if line:
                name, _, value = line.partition("\t")
                result[name] = value

        return result


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-h", "--host", default="127.0.0.1")
    parser.add_argument("-p", "--port", type=int, default=3000)
    parser.add_argument("-U", "--user")
    parser.add_argument("-P", "--password")
    parser.add_argument("command")
    parser.add_argument("args", nargs="*")
    a = parser.parse_args()

    c = Client(a.host, a.port)

    try:
        if a.user:
            c.login(a.user, a.password or "")

        if a.command == "info":
            for k, v in c.info(*(a.args or ["build"])).items():
                print(f"{k}\t{v}")
        elif a.command == "create-user":
            c.create_user(a.args[0], a.args[1], a.args[2:])
        elif a.command == "drop-user":
            c.drop_user(a.args[0])
        elif a.command == "query-users":
            for k, v in c.query_users(*a.args[:1]).items():
                print(f"{k}\t{','.join(v)}")
        elif a.command == "query-roles":
            for k, v in c.query_roles(*a.args[:1]).items():
                print(f"{k}\t{v}")
        else:
            print(f"unknown command {a.command}", file=sys.stderr)
            return 2
    except SecurityError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    finally:
        c.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
