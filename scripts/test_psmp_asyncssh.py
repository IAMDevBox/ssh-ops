#!/usr/bin/env python3
"""Diagnostic: test full PSMP flow with asyncssh.

All commands execute through the SAME shell channel (PSMP requires this).
Flow: auth → shell → reason prompt → MOTD drain → commands via shell

Usage:
    python3 scripts/test_psmp_asyncssh.py <proxy_host> <compound_user> <otp> [reason]

Example:
    python3 scripts/test_psmp_asyncssh.py proxy.example.com user@account@target myotp123
"""

import asyncio
import re
import sys
import time
import uuid

_ANSI_RE = re.compile(r'(\x1b\[[0-9;]*[A-Za-z]|\x1b\][^\x07]*\x07|\x1b\(B)')


def _log(msg):
    print(f"[test] {msg}", file=sys.stderr, flush=True)


async def _read_until(process, pattern, timeout=30):
    """Read from process stdout until pattern is found. Returns all text read."""
    buf = ""
    deadline = time.time() + timeout
    while time.time() < deadline:
        remaining = max(0.5, deadline - time.time())
        try:
            data = await asyncio.wait_for(
                process.stdout.read(4096), timeout=min(2, remaining)
            )
        except asyncio.TimeoutError:
            continue
        if not data:
            break
        buf += data
        if re.search(pattern, buf):
            return buf
    return buf


async def _shell_run(process, command, timeout=30):
    """Run a command through the shell using marker-based output capture."""
    marker = f"__SSHOPS_{uuid.uuid4().hex[:8]}__"
    # Send command with end marker that includes exit code
    wrapped = f"{command}\necho {marker}_RC_$?\n"
    process.stdin.write(wrapped)

    # Read until we see the end marker
    buf = ""
    deadline = time.time() + timeout
    end_pattern = re.compile(rf"{marker}_RC_(\d+)")
    while time.time() < deadline:
        remaining = max(0.5, deadline - time.time())
        try:
            data = await asyncio.wait_for(
                process.stdout.read(4096), timeout=min(2, remaining)
            )
        except asyncio.TimeoutError:
            continue
        if not data:
            break
        buf += data
        m = end_pattern.search(buf)
        if m:
            exit_code = int(m.group(1))
            # Extract output between command echo and marker
            lines = buf.split("\n")
            output_lines = []
            capture = False
            for line in lines:
                clean = _ANSI_RE.sub('', line).strip()
                if end_pattern.search(clean):
                    break
                if capture:
                    # Skip the echo of our wrapped command
                    if marker in clean:
                        continue
                    output_lines.append(clean)
                # Start capturing after the command echo
                if command.strip() in clean and not capture:
                    capture = True
            return "\n".join(output_lines), exit_code

    return _ANSI_RE.sub('', buf), -1


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

    # ── Step 2: Open shell + handle reason prompt ──
    _log("Step 2: Opening shell for reason prompt...")
    process = await conn.create_process(
        term_type="xterm", request_pty=True
    )

    buf = await _read_until(process, r"(?i)reason", timeout=30)
    _log(f"  received prompt ({len(buf)} chars)")
    if "reason" not in buf.lower():
        _log("ERROR: Timeout waiting for reason prompt")
        conn.close()
        return
    process.stdin.write(reason + "\n")
    _log(f"  sent reason: {reason}")

    # ── Step 3: Drain MOTD — wait for shell prompt ($ or #) ──
    _log("Step 3: Draining MOTD...")
    motd = await _read_until(process, r"[$#]\s*$", timeout=15)
    motd_clean = _ANSI_RE.sub('', motd).strip()
    _log(f"Step 3: MOTD ({len(motd_clean)} chars)")
    for line in motd_clean.split('\n')[:5]:
        if line.strip():
            _log(f"  | {line.strip()}")

    # ── Step 4: Run commands through SAME shell ──
    test_commands = ["hostname", "whoami", "uptime", "df -h /"]
    for cmd in test_commands:
        _log(f"Step 4: shell run: {cmd}")
        output, exit_code = await _shell_run(process, cmd, timeout=15)
        _log(f"  output: {output.strip()!r}")
        _log(f"  exit: {exit_code}")

    # ── Done ──
    dt = time.time() - t0
    _log(f"All steps completed ({dt:.1f}s)")
    process.stdin.write_eof()
    process.close()
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
