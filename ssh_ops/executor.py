"""SSH execution engine — connection pool, command execution, file upload."""

import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Generator

import paramiko

from .config import ServerConfig, TaskConfig, check_interactive_command
from .logger import ExecLogger

_ANSI_RE = re.compile(r'(\x1b\[[0-9;]*[A-Za-z]|\x1b\][^\x07]*\x07|\x1b\(B)')
_ENV_KEY_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _exhaust_generator(gen):
    """Consume a generator fully and return (lines, return_value).

    Python's ``for`` loop swallows ``StopIteration`` and discards its
    ``.value``.  This helper manually iterates so that the generator's
    ``return`` value (e.g. an exit code) is captured.
    """
    lines = []
    try:
        while True:
            lines.append(next(gen))
    except StopIteration as e:
        return lines, e.value
    return lines, None  # pragma: no cover


class SSHSession:
    """Wraps a paramiko SSH connection."""

    def __init__(self, server: ServerConfig, keep_alive: int = 60,
                 otp: str | None = None, reason: str = "test"):
        self.server = server
        self.motd = ""
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._lock = threading.Lock()

        if server.proxy:  # pragma: no cover — PSMP proxy path
            proxy = server.proxy
            compound_user = proxy.ssh_username(server.host)

            # Password auth with OTP → PSMP proxy
            import logging
            logging.getLogger("paramiko").setLevel(logging.DEBUG)
            try:
                self.client.connect(
                    hostname=proxy.host,
                    port=proxy.port,
                    username=compound_user,
                    password=otp or "",
                    look_for_keys=False,
                    allow_agent=False,
                )
            except Exception as e:
                raise RuntimeError(
                    f"PSMP auth failed: {type(e).__name__}: {e}\n"
                    f"  proxy={proxy.host}:{proxy.port}\n"
                    f"  username={compound_user}"
                ) from e

            # Handle reason prompt via a temporary shell channel
            shell = self.client.invoke_shell()
            self._expect_send(shell, b'reason', reason, timeout=30)

            # Drain MOTD — wait for shell prompt ($ or #)
            motd_buf = b""
            prompt_deadline = time.time() + 15
            while time.time() < prompt_deadline:
                shell.settimeout(max(1, prompt_deadline - time.time()))
                try:
                    data = shell.recv(4096)
                except Exception:
                    break
                if not data:
                    break
                motd_buf += data
                text = motd_buf.decode(errors="replace")
                stripped = _ANSI_RE.sub('', text).rstrip()
                if stripped and (stripped.endswith('$') or stripped.endswith('#')):
                    break

            self.motd = _ANSI_RE.sub('', motd_buf.decode(errors="replace").strip())
            shell.close()
        else:
            connect_args = {
                "hostname": server.host,
                "port": server.port,
                "username": server.username,
            }
            if server.key_file:
                key_path = Path(server.key_file).expanduser().resolve()
                connect_args["key_filename"] = str(key_path)
            elif server.password:
                connect_args["password"] = server.password

            self.client.connect(**connect_args)

        # Enable keepalive
        transport = self.client.get_transport()
        if transport:
            transport.set_keepalive(keep_alive)

    @staticmethod
    def _expect_send(shell, expect: bytes, response: str,
                         timeout: int = 15):  # pragma: no cover — PSMP only
        """Read from shell until `expect` appears, then send response."""
        buf = b""
        deadline = time.time() + timeout
        while time.time() < deadline:
            shell.settimeout(max(1, deadline - time.time()))
            try:
                data = shell.recv(4096)
            except Exception:
                break
            if not data:
                break
            buf += data
            if expect in buf.lower():
                shell.sendall(f"{response}\n".encode())
                return
        raise RuntimeError(f"Timeout waiting for '{expect.decode()}' prompt")

    def exec_command(self, command: str, timeout: int = 120,
                     env: dict | None = None) -> Generator[str, None, int]:
        """Execute command and yield output lines. Returns exit code via StopIteration."""
        # Collect all output inside the lock so it's released promptly
        with self._lock:
            # Build env prefix
            env_prefix = ""
            if env:
                for k in env:
                    if not _ENV_KEY_RE.match(k):
                        raise ValueError(f"Invalid env variable name: '{k}'")
                parts = [f"export {k}={_shell_quote(v)}" for k, v in env.items()]
                env_prefix = " && ".join(parts) + " && "

            full_cmd = f"{env_prefix}{command}"
            stdin, stdout, stderr = self.client.exec_command(
                full_cmd, timeout=timeout
            )
            stdin.close()

            # Read stdout and stderr concurrently to preserve ordering
            output_lines = []
            stderr_lines = []

            def _read_stderr():
                for line in iter(stderr.readline, ""):
                    stderr_lines.append(f"[STDERR] {line.rstrip(chr(10))}")

            err_thread = threading.Thread(target=_read_stderr, daemon=True)
            err_thread.start()

            for line in iter(stdout.readline, ""):
                output_lines.append(line.rstrip("\n"))

            err_thread.join(timeout=10)
            output_lines.extend(stderr_lines)

            exit_code = stdout.channel.recv_exit_status()

        # Yield outside the lock — safe even if caller abandons the generator
        for text in output_lines:
            yield text
        return exit_code

    def upload_file(self, local_path: str, remote_path: str,
                    mode: str | int | None = None) -> None:
        """Upload a local file to remote server via SFTP."""
        with self._lock:
            sftp = self.client.open_sftp()
            try:
                sftp.put(local_path, remote_path)
                if mode is not None:
                    if isinstance(mode, int):
                        sftp.chmod(remote_path, mode)
                    else:
                        sftp.chmod(remote_path, int(str(mode), 8))
            finally:
                sftp.close()

    def download_file(self, remote_path: str, local_path: str) -> None:
        """Download a file from remote server via SFTP."""
        with self._lock:
            sftp = self.client.open_sftp()
            try:
                Path(local_path).parent.mkdir(parents=True, exist_ok=True)
                sftp.get(remote_path, local_path)
            finally:
                sftp.close()

    def is_alive(self) -> bool:
        transport = self.client.get_transport()
        return transport is not None and transport.is_active()

    def close(self):
        self.client.close()


class ConnectionPool:
    """Manages SSH connections to multiple servers."""

    def __init__(self, keep_alive: int = 60):
        self.keep_alive = keep_alive
        self._sessions: dict[str, SSHSession] = {}
        self._lock = threading.Lock()

    def connect(self, server: ServerConfig, otp: str | None = None) -> SSHSession:
        # Quick check under lock — return existing live session
        with self._lock:
            if server.name in self._sessions:
                session = self._sessions[server.name]
                if session.is_alive():
                    return session
                session.close()
                del self._sessions[server.name]

        # Create session WITHOUT holding the pool lock so parallel
        # connections can proceed concurrently (critical for OTP expiry).
        session = SSHSession(server, self.keep_alive, otp=otp)

        # Store under lock
        with self._lock:
            # Another thread may have connected same server meanwhile
            if server.name in self._sessions:  # pragma: no cover — race condition guard
                existing = self._sessions[server.name]
                if existing.is_alive():
                    session.close()
                    return existing
                existing.close()
            self._sessions[server.name] = session
            return session

    def get_session(self, server_name: str) -> SSHSession | None:
        with self._lock:
            session = self._sessions.get(server_name)
            if session and session.is_alive():
                return session
            return None

    def disconnect(self, server_name: str):
        with self._lock:
            session = self._sessions.pop(server_name, None)
            if session:
                session.close()

    def disconnect_all(self):
        with self._lock:
            for session in self._sessions.values():
                session.close()
            self._sessions.clear()

    def connected_servers(self) -> list[str]:
        with self._lock:
            return [name for name, s in self._sessions.items() if s.is_alive()]


class TaskExecutor:
    """Executes tasks on servers."""

    def __init__(self, pool: ConnectionPool, logger: ExecLogger):
        self.pool = pool
        self.logger = logger

    def run_task(self, server: ServerConfig, task: TaskConfig) -> bool:
        """Run a single task on a server. Returns True on success."""
        session = self.pool.get_session(server.name)
        if not session:
            self.logger.error(f"[{server.name}] Not connected")
            return False

        log_path = self.logger.create_exec_log(server.name, task.name)
        detail = task.command or task.src or task.name
        ts = datetime.now().strftime("%H:%M:%S")
        self.logger.write_exec_log(log_path, f"\n[{ts}] {detail}\n")
        header_text = f"[{server.name}] {detail}"
        pad = max(0, 50 - len(header_text) - 4)
        self.logger.info(f"── {header_text} {'─' * pad}")

        try:
            if task.type == "upload":
                return self._do_upload(session, server, task, log_path)
            elif task.type == "command":
                return self._do_command(session, server, task, log_path)
            elif task.type == "script":
                return self._do_script(session, server, task, log_path)
            else:
                self.logger.error(f"[{server.name}] Unknown task type: {task.type}")
                return False
        except Exception as e:
            msg = f"[{server.name}] '{detail}' failed: {e}"
            self.logger.error(msg)
            self.logger.write_exec_log(log_path, f"\n{msg}\n")
            return False

    def _do_upload(self, session: SSHSession, server: ServerConfig,
                   task: TaskConfig, log_path: Path) -> bool:
        """Upload local file to remote. dest must be absolute file path, not directory."""
        src = task.src
        if not src:
            self.logger.error(f"[{server.name}] Upload task missing src")
            return False
        if not task.dest:
            self.logger.error(f"[{server.name}] Upload task missing dest")
            return False
        if not Path(src).exists():
            self.logger.error(f"[{server.name}] Local file not found: {src}")
            return False
        if not task.dest.startswith("/"):
            self.logger.error(f"[{server.name}] dest must be absolute path: {task.dest}")
            return False
        if task.dest.endswith("/") or task.dest.endswith("\\"):
            self.logger.error(f"[{server.name}] dest must be file path, not directory: {task.dest}")
            return False

        session.upload_file(src, task.dest, task.mode)
        self.logger.info(f"{src} -> {task.dest}")
        ts = datetime.now().strftime("%H:%M:%S")
        self.logger.write_exec_log(log_path, f"[{ts}] {src} -> {task.dest}\n")
        self.logger.write_exec_log(log_path, f"[{ts}] => OK\n")
        return True

    def _do_command(self, session: SSHSession, server: ServerConfig,
                    task: TaskConfig, log_path: Path) -> bool:
        if not task.command or not task.command.strip():
            self.logger.error(f"[{server.name}] Command task has empty command")
            return False
        interactive_err = check_interactive_command(task.command)
        if interactive_err:
            self.logger.error(f"[{server.name}] {interactive_err}")
            return False
        gen = session.exec_command(task.command, task.timeout, task.env)
        lines, exit_code = _exhaust_generator(gen)
        exit_code = exit_code or 0
        for line in lines:
            self.logger.info(f"[{server.name}] {line}")
            ts = datetime.now().strftime("%H:%M:%S")
            self.logger.write_exec_log(log_path, f"[{ts}] {line}\n")

        if exit_code != 0:
            status = f"FAILED (exit={exit_code})"
            self.logger.info(f"✗ {status}")
        ts = datetime.now().strftime("%H:%M:%S")
        self.logger.write_exec_log(
            log_path, f"[{ts}] => {'OK' if exit_code == 0 else status}\n")
        return exit_code == 0

    def _do_script(self, session: SSHSession, server: ServerConfig,
                   task: TaskConfig, log_path: Path) -> bool:
        """Upload local script, execute remotely, then remove."""
        src = task.src
        if not src:
            self.logger.error(f"[{server.name}] Script task missing src")
            return False
        if not Path(src).exists():
            self.logger.error(f"[{server.name}] Local script not found: {src}")
            return False

        remote_tmp = f"/tmp/_ssh_ops_{Path(src).name}"
        session.upload_file(src, remote_tmp, "0755")

        cmd = _shell_quote(remote_tmp)
        if task.args:
            import shlex as _shlex
            cmd += " " + " ".join(_shell_quote(a) for a in _shlex.split(str(task.args)))

        gen = session.exec_command(cmd, task.timeout, task.env)
        lines, exit_code = _exhaust_generator(gen)
        exit_code = exit_code or 0
        for line in lines:
            self.logger.info(f"[{server.name}] {line}")
            ts = datetime.now().strftime("%H:%M:%S")
            self.logger.write_exec_log(log_path, f"[{ts}] {line}\n")

        # Cleanup
        try:
            _exhaust_generator(session.exec_command(f"rm -f {remote_tmp}", timeout=10))
        except Exception:  # pragma: no cover — defensive cleanup
            pass

        if exit_code != 0:
            status = f"FAILED (exit={exit_code})"
            self.logger.info(f"✗ {status}")
        ts = datetime.now().strftime("%H:%M:%S")
        self.logger.write_exec_log(
            log_path, f"[{ts}] => {'OK' if exit_code == 0 else status}\n")
        return exit_code == 0

    def run_all_tasks(self, servers: list[ServerConfig],
                      tasks: list[TaskConfig], serial: bool = True,
                      progress_callback=None,
                      cancel_event: threading.Event | None = None) -> dict:
        """Run all tasks in order on each server.

        serial=True: one server at a time (server1: all tasks, then server2: all tasks)
        serial=False: all servers in parallel, each running tasks in order
        progress_callback: optional callable(event_dict) for real-time progress updates
        cancel_event: optional threading.Event; when set, remaining tasks are skipped
        """
        results = {}

        if serial:
            for si, server in enumerate(servers):
                if cancel_event and cancel_event.is_set():
                    break
                if progress_callback:
                    progress_callback({"event": "server_start", "server": server.name,
                                       "server_index": si, "server_total": len(servers)})
                results[server.name] = self._run_tasks_on_server(
                    server, tasks, progress_callback=progress_callback,
                    cancel_event=cancel_event)
        else:
            results_lock = threading.Lock()
            threads = {}
            for server in servers:
                def _worker(s=server):
                    task_results = self._run_tasks_on_server(
                        s, tasks, progress_callback=progress_callback,
                        cancel_event=cancel_event)
                    with results_lock:
                        results[s.name] = task_results
                t = threading.Thread(target=_worker)
                threads[server.name] = t
                t.start()
            for t in threads.values():
                t.join()

        return results

    def _run_tasks_on_server(self, server: ServerConfig,
                             tasks: list[TaskConfig],
                             progress_callback=None,
                             cancel_event: threading.Event | None = None) -> list[dict]:
        """Run tasks in order on one server. Stop on first failure or cancel."""
        task_results = []
        for ti, task in enumerate(tasks):
            if cancel_event and cancel_event.is_set():
                if progress_callback:
                    progress_callback({"event": "task_done", "server": server.name,
                                       "task": task.name, "task_index": ti, "task_total": len(tasks),
                                       "success": False, "cancelled": True})
                task_results.append({"task": task.name, "success": False, "cancelled": True})
                continue
            if progress_callback:
                progress_callback({"event": "task_start", "server": server.name,
                                   "task": task.name, "task_index": ti, "task_total": len(tasks)})
            success = self.run_task(server, task)
            task_results.append({"task": task.name, "success": success})
            if progress_callback:
                progress_callback({"event": "task_done", "server": server.name,
                                   "task": task.name, "task_index": ti, "task_total": len(tasks),
                                   "success": success})
            if not success:
                detail = task.command or task.src or task.name
                self.logger.error(
                    f"[{server.name}] Stopping — '{detail}' failed"
                )
                break
        return task_results


def _shell_quote(s: str) -> str:
    """Shell-safe quoting using shlex."""
    import shlex
    return shlex.quote(str(s))
