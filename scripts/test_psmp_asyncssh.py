#!/usr/bin/env python3
"""Diagnostic: test PSMP proxy auth with asyncssh (alternative to paramiko).

Usage:
    python3 scripts/test_psmp_asyncssh.py <proxy_host> <compound_user> <otp>

Example:
    python3 scripts/test_psmp_asyncssh.py proxy.example.com user@account@target otp123
"""

import asyncio
import logging
import sys
import time


async def test_asyncssh(proxy_host, compound_user, otp):
    import asyncssh

    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr,
                        format="%(name)s: %(message)s")

    print(f"[asyncssh] host={proxy_host}:22", file=sys.stderr)
    print(f"[asyncssh] user={compound_user}", file=sys.stderr)
    print(f"[asyncssh] otp length={len(otp)}", file=sys.stderr)

    t0 = time.time()
    try:
        conn = await asyncssh.connect(
            proxy_host,
            port=22,
            username=compound_user,
            password=otp,
            known_hosts=None,
            preferred_auth="password",
        )
        dt = time.time() - t0
        print(f"[asyncssh] AUTH SUCCEEDED ({dt:.1f}s)", file=sys.stderr)

        # Try running a command
        result = await conn.run("hostname", check=True)
        print(f"[asyncssh] hostname: {result.stdout.strip()}", file=sys.stderr)
        conn.close()
    except Exception as e:
        dt = time.time() - t0
        print(f"[asyncssh] AUTH FAILED ({dt:.1f}s): {type(e).__name__}: {e}",
              file=sys.stderr)
        raise


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(__doc__)
        sys.exit(1)
    asyncio.run(test_asyncssh(sys.argv[1], sys.argv[2], sys.argv[3]))
