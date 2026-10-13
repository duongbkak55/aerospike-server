#!/usr/bin/env python3
"""Minimal info-protocol client for ext-lab tests (no Aerospike tools needed).

Usage: asinfo.py [-h host] [-p port] [command ...]
Prints one response per command. Exits non-zero if the node can't be reached.
"""

import argparse
import socket
import struct
import sys

PROTO_VERSION = 2
PROTO_TYPE_INFO = 1


def info(host, port, commands, timeout=5.0):
    body = "".join(c + "\n" for c in commands).encode()
    header = struct.pack(">Q", (PROTO_VERSION << 56) | (PROTO_TYPE_INFO << 48)
                         | len(body))

    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.sendall(header + body)
        size = struct.unpack(">Q", recv_all(sock, 8))[0] & 0xFFFFFFFFFFFF
        data = recv_all(sock, size).decode(errors="replace")

    result = {}

    for line in data.split("\n"):
        if line:
            name, _, value = line.partition("\t")
            result[name] = value

    return result


def recv_all(sock, n):
    buf = b""

    while len(buf) < n:
        chunk = sock.recv(n - len(buf))

        if not chunk:
            raise ConnectionError("connection closed")

        buf += chunk

    return buf


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-h", "--host", default="127.0.0.1")
    parser.add_argument("-p", "--port", type=int, default=3000)
    parser.add_argument("commands", nargs="*", default=["build"])
    args = parser.parse_args()

    try:
        result = info(args.host, args.port, args.commands)
    except OSError as e:
        print(f"error: {args.host}:{args.port}: {e}", file=sys.stderr)
        return 1

    for c in args.commands:
        print(result.get(c, ""))

    return 0


if __name__ == "__main__":
    sys.exit(main())
