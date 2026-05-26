#!/usr/bin/env python3
"""TCP black-hole server for Snowflake login timeout repro.

Accepts connections, drains client writes, and never sends HTTP headers back.
This mimics a slow or unresponsive Snowflake auth endpoint and triggers errors
like:

  context deadline exceeded (Client.Timeout exceeded while awaiting headers)

Matches the listener in `snowflake_context_deadline_exceeded.rs`.
"""

from __future__ import annotations

import argparse
import socket
import sys
import threading

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9999


def handle(conn: socket.socket, addr: tuple[str, int]) -> None:
    print(f"accepted {addr}", flush=True)
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
    except OSError:
        pass
    finally:
        conn.close()
        print(f"closed {addr}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help=f"bind address (default: {DEFAULT_HOST})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"bind port (default: {DEFAULT_PORT}; must match profiles.yml)",
    )
    args = parser.parse_args()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((args.host, args.port))
        server.listen()
        print(f"blackhole listening on {args.host}:{args.port}", flush=True)
        print("Ctrl+C to stop", flush=True)

        while True:
            try:
                conn, addr = server.accept()
            except KeyboardInterrupt:
                print("\nstopped", flush=True)
                return 0
            threading.Thread(
                target=handle,
                args=(conn, addr),
                daemon=True,
            ).start()


if __name__ == "__main__":
    sys.exit(main())
