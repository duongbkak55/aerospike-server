#!/usr/bin/env python3
#
# Minimal TCP forwarder for XDR tests - point a DC at the proxy, then stop the
# proxy process to partition the DCs and start it again to heal.
#
# Usage: tcpproxy.py <listen-port> <target-port>   (both on 127.0.0.1)

import asyncio
import sys


async def pipe(reader, writer):
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        writer.close()


async def handle(client_reader, client_writer, target_port):
    try:
        server_reader, server_writer = await asyncio.open_connection(
            "127.0.0.1", target_port)
    except OSError:
        client_writer.close()
        return

    await asyncio.gather(pipe(client_reader, server_writer),
                         pipe(server_reader, client_writer))


async def main():
    listen_port, target_port = int(sys.argv[1]), int(sys.argv[2])
    server = await asyncio.start_server(
        lambda r, w: handle(r, w, target_port), "127.0.0.1", listen_port,
        reuse_address=True)

    async with server:
        await server.serve_forever()


asyncio.run(main())
