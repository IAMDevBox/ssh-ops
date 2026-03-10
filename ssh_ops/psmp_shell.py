"""PSMP proxy shell — asyncssh-based persistent shell for PSMP connections.

Provides a synchronous interface over asyncssh for the PSMP proxy path.
All commands run through a single persistent shell channel using
marker-based output capture, because PSMP only supports shell channels
(not SSH exec or SFTP).
"""

import asyncio
import re
import threading
import uuid
from typing import Generator

import asyncssh

_ANSI_RE = re.compile(r'(\x1b\[[0-9;]*[A-Za-z]|\x1b\][^\x07]*\x07|\x1b\(B)')


class PsmpShell:
    """Wraps an asyncssh connection + persistent shell for PSMP proxy."""

    def __init__(self, proxy_host: str, proxy_port: int,
                 compound_user: str, otp: str,
                 reason: str = "test", keep_alive: int = 60):
        self.motd = ""
        self._conn = None
        self._process = None
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, daemon=True
        )
        self._thread.start()
        self._cmd_lock = threading.Lock()

        # Connect synchronously (blocks until shell is ready)
        future = asyncio.run_coroutine_threadsafe(
            self._async_connect(
                proxy_host, proxy_port, compound_user, otp,
                reason, keep_alive
            ),
            self._loop,
        )
        future.result(timeout=90)  # auth + reason + MOTD

    async def _async_connect(self, host, port, user, otp, reason, keep_alive):
        self._conn = await asyncssh.connect(
            host, port=port,
            username=user, password=otp,
            known_hosts=None,
            preferred_auth="password",
            keepalive_interval=keep_alive,
        )

        self._process = await self._conn.create_process(
            term_type="xterm", request_pty=True
        )

        # Handle reason prompt
        buf = await self._read_until(r"(?i)reason", timeout=30)
        if "reason" not in buf.lower():
            raise RuntimeError("Timeout waiting for reason prompt")
        self._process.stdin.write(reason + "\n")

        # Drain MOTD — wait for shell prompt
        motd = await self._read_until(r"[$#]\s*$", timeout=15)
        self.motd = _ANSI_RE.sub('', motd).strip()

    async def _read_until(self, pattern: str, timeout: int = 30) -> str:
        buf = ""
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            remaining = max(0.5, deadline - asyncio.get_event_loop().time())
            try:
                data = await asyncio.wait_for(
                    self._process.stdout.read(4096),
                    timeout=min(2, remaining),
                )
            except asyncio.TimeoutError:
                if re.search(pattern, buf):
                    return buf
                continue
            if not data:
                break
            buf += data
            if re.search(pattern, buf):
                return buf
        return buf

    async def _async_run(self, command: str, timeout: int = 120) -> tuple:
        """Run command via shell, return (lines, exit_code)."""
        marker = f"__SSHOPS_{uuid.uuid4().hex[:8]}__"
        # Leading space avoids bash history; echo marker captures exit code
        self._process.stdin.write(
            f" {command}\necho {marker}_RC_$?\n"
        )

        buf = ""
        end_re = re.compile(rf"{marker}_RC_(\d+)")
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            remaining = max(0.5, deadline - asyncio.get_event_loop().time())
            try:
                data = await asyncio.wait_for(
                    self._process.stdout.read(4096),
                    timeout=min(2, remaining),
                )
            except asyncio.TimeoutError:
                if end_re.search(buf):
                    break
                continue
            if not data:
                break
            buf += data
            if end_re.search(buf):
                break

        # Parse output
        m = end_re.search(buf)
        exit_code = int(m.group(1)) if m else -1

        lines = buf.split("\n")
        output = []
        capture = False
        for line in lines:
            clean = _ANSI_RE.sub('', line).strip()
            if end_re.search(clean):
                break
            if capture:
                if marker in clean:
                    continue
                output.append(clean)
            if command.strip() in clean and not capture:
                capture = True

        return output, exit_code

    def run_command(self, command: str, timeout: int = 120,
                    env: dict | None = None) -> Generator[str, None, int]:
        """Run command through persistent shell. Yields output lines."""
        from .executor import _shell_quote, _ENV_KEY_RE

        env_prefix = ""
        if env:
            for k in env:
                if not _ENV_KEY_RE.match(k):
                    raise ValueError(f"Invalid env variable name: '{k}'")
            parts = [f"export {k}={_shell_quote(v)}" for k, v in env.items()]
            env_prefix = " && ".join(parts) + " && "

        full_cmd = f"{env_prefix}{command}"

        with self._cmd_lock:
            future = asyncio.run_coroutine_threadsafe(
                self._async_run(full_cmd, timeout), self._loop
            )
            lines, exit_code = future.result(timeout=timeout + 10)

        for line in lines:
            yield line
        return exit_code

    def is_alive(self) -> bool:
        if not self._conn or not self._process:
            return False
        try:
            return (
                self._thread.is_alive()
                and self._conn._transport is not None
                and not self._conn._transport.is_closing()
            )
        except Exception:
            return False

    def close(self):
        if self._process:
            try:
                self._process.stdin.write_eof()
                self._process.close()
            except Exception:
                pass
            self._process = None
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
        if self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5)
