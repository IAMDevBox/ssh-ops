"""PSMP proxy shell — asyncssh-based persistent shell for PSMP connections.

Provides a synchronous interface over asyncssh for the PSMP proxy path.
All commands run through a single persistent shell channel using
marker-based output capture, because PSMP only supports shell channels
(not SSH exec or SFTP).

History-clean markers
---------------------
Instead of ``echo START; cmd; echo END$?`` (which pollutes bash history
with marker noise), we use PS0 and PROMPT_COMMAND to emit markers:

- **PS0** is printed by bash *after* a command is read, *before* it
  executes.  We set it to ``printf`` the start marker.
- **PROMPT_COMMAND** runs *after* a command finishes, *before* the next
  prompt.  We set it to ``printf`` the end marker + ``$?`` (exit code).

Because PS0 and PROMPT_COMMAND are *variable assignments* (set once),
they don't create per-command history entries.  The only history entry
for each command is the command itself.
"""

import asyncio
import logging
import re
import threading
import uuid
from typing import Generator

import asyncssh

logger = logging.getLogger(__name__)

_ANSI_RE = re.compile(r'(\x1b\[[0-9;]*[A-Za-z]|\x1b\][^\x07]*\x07|\x1b\(B)')

# Stable prefix — the per-session suffix is appended at connect time.
_MARKER_PREFIX = "__SSHOPS_"


class PsmpShell:
    """Wraps an asyncssh connection + persistent shell for PSMP proxy."""

    def __init__(self, proxy_host: str, proxy_port: int,
                 compound_user: str, otp: str,
                 reason: str = "test", keep_alive: int = 60):
        self.motd = ""
        self._conn = None
        self._process = None
        self._sftp = None  # SFTP client if supported
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, daemon=True
        )
        self._thread.start()
        self._cmd_lock = threading.Lock()

        # Session-unique marker tokens (set during _async_connect)
        self._start_marker: str = ""
        self._end_marker: str = ""
        self._end_re: re.Pattern | None = None

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

        # -- Install marker machinery via PS0 + PROMPT_COMMAND -----------
        # These are variable assignments — they create ONE history entry
        # (the assignment itself) rather than polluting every command.
        session_id = uuid.uuid4().hex[:8]
        self._start_marker = f"{_MARKER_PREFIX}{session_id}__START"
        self._end_marker = f"{_MARKER_PREFIX}{session_id}__RC_"
        self._end_re = re.compile(
            rf"{re.escape(self._end_marker)}(\d+)"
        )

        # Use printf (not echo) so output is exact, no trailing newline
        # surprises.  The markers go to stdout which we're reading.
        # Single line keeps history to one entry for the setup.
        setup = (
            f"PS0=$'\\n{self._start_marker}\\n'; "
            f"PROMPT_COMMAND='printf \"\\n{self._end_marker}%d\\n\" $?'"
            "\n"
        )
        self._process.stdin.write(setup)

        # Drain the setup echo + PROMPT_COMMAND end-marker + prompt.
        # PROMPT_COMMAND fires for the setup command itself, emitting an
        # end marker that we need to consume before real commands start.
        await self._read_until(
            rf"{re.escape(self._end_marker)}\d+", timeout=10
        )
        # Now drain to the next prompt
        await self._read_until(r"[$#]\s*$", timeout=10)

        # Try to open SFTP — avoids shell-based file transfer if supported
        try:
            self._sftp = await self._conn.start_sftp_client()
            logger.info("PSMP SFTP channel opened")
        except Exception as exc:
            self._sftp = None
            logger.warning("PSMP SFTP not available: %s — file ops will use shell", exc)
            logger.debug("SFTP error details:", exc_info=True)

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
        """Run command via shell, return (lines, exit_code).

        Flow (with PS0 / PROMPT_COMMAND already installed):
          1. We write ``command\\n`` — this is the *only* history entry.
          2. Bash prints PS0 (contains start marker) before executing.
          3. Command runs; its stdout arrives.
          4. PROMPT_COMMAND fires and prints end marker + $?.
          5. We capture everything between the two markers.
        """
        # Just send the command — markers are injected by PS0/PROMPT_COMMAND
        self._process.stdin.write(command + "\n")

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
                if self._end_re.search(buf):
                    break
                continue
            if not data:
                break
            buf += data
            if self._end_re.search(buf):
                break

        # Parse output — capture lines between start and end markers
        m = self._end_re.search(buf)
        exit_code = int(m.group(1)) if m else -1

        lines = buf.split("\n")
        output = []
        capture = False
        for line in lines:
            stripped = _ANSI_RE.sub('', line).strip()
            content = _ANSI_RE.sub('', line).rstrip('\r')
            if self._end_re.search(stripped):
                break
            if capture:
                if _MARKER_PREFIX in stripped:
                    continue
                output.append(content)
            elif self._start_marker in stripped:
                capture = True
                # The start marker line itself is consumed; the next line
                # may be the PTY echo of the command (prompt + cmd).  We
                # skip that below.

        # Strip leading PTY command echo (prompt + typed command)
        if output:
            first = _ANSI_RE.sub('', output[0]).strip()
            if re.search(r'[$#]\s', first):
                output = output[1:]

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

    def upload_file(self, local_path: str, remote_path: str,
                    mode: str | int | None = None) -> None:
        """Upload file — SFTP if available, otherwise base64 shell transfer."""
        if self._sftp:
            future = asyncio.run_coroutine_threadsafe(
                self._sftp_upload(local_path, remote_path, mode), self._loop)
            future.result(timeout=130)
            return

        # Fallback: base64 shell transfer
        import base64
        from pathlib import Path

        data = Path(local_path).read_bytes()
        b64 = base64.b64encode(data).decode('ascii')

        chunk_size = 4096
        chunks = [b64[i:i + chunk_size] for i in range(0, len(b64), chunk_size)]

        tmp_b64 = remote_path + '._b64tmp'
        cmds = [f"rm -f {tmp_b64}"]
        for chunk in chunks:
            cmds.append(f"printf '%s' '{chunk}' >> {tmp_b64}")
        cmds.append(f"base64 -d {tmp_b64} > {remote_path}")
        cmds.append(f"rm -f {tmp_b64}")
        if mode is not None:
            mode_str = str(mode) if isinstance(mode, int) else mode
            cmds.append(f"chmod {mode_str} {remote_path}")

        full_cmd = " && ".join(cmds)
        with self._cmd_lock:
            future = asyncio.run_coroutine_threadsafe(
                self._async_run(full_cmd, timeout=120), self._loop)
            _, exit_code = future.result(timeout=130)
            if exit_code != 0:
                raise RuntimeError(
                    f"Upload failed (exit={exit_code}): {local_path} -> {remote_path}")

    async def _sftp_upload(self, local_path: str, remote_path: str,
                           mode: str | int | None = None) -> None:
        await self._sftp.put(local_path, remote_path)
        if mode is not None:
            perms = int(str(mode), 8) if isinstance(mode, str) else mode
            await self._sftp.chmod(remote_path, perms)

    def download_file(self, remote_path: str, local_path: str) -> None:
        """Download file — SFTP if available, otherwise base64 shell transfer."""
        from pathlib import Path
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        if self._sftp:
            future = asyncio.run_coroutine_threadsafe(
                self._sftp.get(remote_path, local_path), self._loop)
            future.result(timeout=130)
            return

        # Fallback: base64 shell transfer
        import base64
        cmd = f"base64 {remote_path}"
        with self._cmd_lock:
            future = asyncio.run_coroutine_threadsafe(
                self._async_run(cmd, timeout=120), self._loop)
            lines, exit_code = future.result(timeout=130)
            if exit_code != 0:
                raise RuntimeError(
                    f"Download failed (exit={exit_code}): {remote_path}")

        b64_text = "".join(line.strip() for line in lines)
        Path(local_path).write_bytes(base64.b64decode(b64_text))

    @property
    def has_sftp(self) -> bool:
        return self._sftp is not None

    def sftp_read_file(self, remote_path: str) -> str:
        """Read a text file via SFTP. Raises on directory or binary."""
        future = asyncio.run_coroutine_threadsafe(
            self._sftp_read(remote_path), self._loop)
        return future.result(timeout=60)

    async def _sftp_read(self, remote_path: str) -> str:
        stat = await self._sftp.stat(remote_path)
        import asyncssh
        if asyncssh.FILEXFER_TYPE_DIRECTORY == (stat.type if hasattr(stat, 'type') else None):
            raise IsADirectoryError(remote_path)
        # Also check via permissions
        if stat.permissions is not None and (stat.permissions & 0o40000):
            raise IsADirectoryError(remote_path)
        async with self._sftp.open(remote_path, 'rb') as f:
            data = await f.read()
        # Check binary
        if b'\x00' in data[:8192]:
            raise ValueError("Binary file — cannot read as text")
        return data.decode('utf-8')

    def sftp_write_file(self, remote_path: str, content: str) -> None:
        """Write a text file via SFTP."""
        future = asyncio.run_coroutine_threadsafe(
            self._sftp_write(remote_path, content), self._loop)
        future.result(timeout=60)

    async def _sftp_write(self, remote_path: str, content: str) -> None:
        async with self._sftp.open(remote_path, 'wb') as f:
            await f.write(content.encode('utf-8'))

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
        if self._sftp:
            try:
                self._sftp.exit()
            except Exception:
                pass
            self._sftp = None
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
