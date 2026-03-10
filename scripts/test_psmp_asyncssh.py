#!/usr/bin/env python3
"""Diagnostic: test full PSMP flow with asyncssh.

Tests: auth → reason prompt → MOTD drain → exec_command → SFTP

Usage:
    python3 scripts/test_psmp_asyncssh.py <proxy_host> <compound_user> <otp> [reason]

Example:
    python3 scripts/test_psmp_asyncssh.py proxy.example.com user@account@target myotp123
    python3 scripts/test_psmp_asyncssh.py proxy.example.com user@account@target myotp123 "maintenance"
"""

import asyncio
import re
import sys
import time

_ANSI_RE = re.compile(r'(\x1b\[[0-9;]*[A-Za-z]|\x1b\][^\x07]*\x07|\x1b\(B)')


def _log(msg):
    print(f"[test] {msg}", file=sys.stderr, flush=True)


async def test_psmp(proxy_host, compound_user, otp, reason="test"):
    import asyncssh

    _log(f"host={proxy_host}:22")
    _log(f"user={compound_user}")
    _log(f"otp length={len(otp)}")
    _log(f"reason={reason}")

    # ── Step 1: Connect + auth ──
    t0 = time.time()
    _log("Step 1: Connecting...")
    conn = await asyncssh.connect(
        proxy_host,
        port=22,
        username=compound_user,
        password=otp,
        known_hosts=None,
        preferred_auth="password",
    )
    _log(f"Step 1: Auth succeeded ({time.time()-t0:.1f}s)")

    # ── Step 2: Handle reason prompt via shell ──
    _log("Step 2: Opening shell for reason prompt...")
    process = await conn.create_process(
        term_type="xterm", request_pty=True
    )

    # Read until "reason" appears, then send reason
    buf = ""
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            data = await asyncio.wait_for(
                process.stdout.read(4096), timeout=2
            )
        except asyncio.TimeoutError:
            continue
        if not data:
            break
        buf += data
        _log(f"  shell recv: {_ANSI_RE.sub('', data).strip()!r}")
        if "reason" in buf.lower():
            process.stdin.write(reason + "\n")
            _log(f"  sent reason: {reason}")
            break
    else:
        _log("ERROR: Timeout waiting for reason prompt")
        conn.close()
        return

    # ── Step 3: Drain MOTD — wait for shell prompt ($ or #) ──
    _log("Step 3: Draining MOTD...")
    motd_buf = ""
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            data = await asyncio.wait_for(
                process.stdout.read(4096), timeout=2
            )
        except asyncio.TimeoutError:
            # No more data, check if we already have a prompt
            stripped = _ANSI_RE.sub('', motd_buf).rstrip()
            if stripped and (stripped.endswith('$') or stripped.endswith('#')):
                break
            continue
        if not data:
            break
        motd_buf += data
        stripped = _ANSI_RE.sub('', motd_buf).rstrip()
        if stripped and (stripped.endswith('$') or stripped.endswith('#')):
            break

    motd_clean = _ANSI_RE.sub('', motd_buf).strip()
    _log(f"Step 3: MOTD ({len(motd_clean)} chars): {motd_clean[:200]}")

    # Close shell channel
    process.stdin.write_eof()
    process.close()

    # ── Step 4: Test exec_command ──
    _log("Step 4: Testing exec_command (hostname)...")
    try:
        result = await conn.run("hostname", check=False, timeout=10)
        _log(f"  stdout: {result.stdout.strip()!r}")
        _log(f"  stderr: {result.stderr.strip()!r}")
        _log(f"  exit: {result.exit_status}")
    except Exception as e:
        _log(f"  exec_command failed: {type(e).__name__}: {e}")

    # ── Step 5: Test another command ──
    _log("Step 5: Testing exec_command (whoami)...")
    try:
        result = await conn.run("whoami", check=False, timeout=10)
        _log(f"  stdout: {result.stdout.strip()!r}")
        _log(f"  exit: {result.exit_status}")
    except Exception as e:
        _log(f"  exec_command failed: {type(e).__name__}: {e}")

    # ── Step 6: Test SFTP ──
    _log("Step 6: Testing SFTP (list /tmp)...")
    try:
        async with conn.start_sftp_client() as sftp:
            entries = await sftp.listdir("/tmp")
            _log(f"  /tmp has {len(entries)} entries")
    except Exception as e:
        _log(f"  SFTP failed: {type(e).__name__}: {e}")

    # ── Done ──
    dt = time.time() - t0
    _log(f"All steps completed ({dt:.1f}s)")
    conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(1)
    reason = sys.argv[4] if len(sys.argv) > 4 else "test"
    try:
        asyncio.run(test_psmp(sys.argv[1], sys.argv[2], sys.argv[3], reason))
    except Exception as e:
        _log(f"FAILED: {type(e).__name__}: {e}")
        sys.exit(1)
