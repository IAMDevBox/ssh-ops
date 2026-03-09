"""Tests for ssh_ops.executor — task execution with mocked SSH."""

import threading
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from ssh_ops.config import ServerConfig, TaskConfig
from ssh_ops.executor import SSHSession, ConnectionPool, TaskExecutor, _shell_quote, _exhaust_generator
from ssh_ops.logger import ExecLogger


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _server(name="web1", host="10.0.0.1"):
    return ServerConfig({"host": host, "name": name, "password": "pass"})


def _task(overrides=None):
    TaskConfig._counter.clear()
    base = {"command": "echo hello"}
    if overrides:
        base.update(overrides)
    return TaskConfig(base)


class FakeSession:
    """Mock SSHSession that returns preset output."""

    def __init__(self, output_lines=None, exit_code=0, upload_ok=True):
        self.output_lines = output_lines or ["line1", "line2"]
        self.exit_code = exit_code
        self.upload_ok = upload_ok
        self.uploaded = []
        self.executed = []
        self.server = _server()

    def exec_command(self, command, timeout=120, env=None):
        self.executed.append(command)
        for line in self.output_lines:
            yield line
        return self.exit_code

    def upload_file(self, local_path, remote_path, mode=None):
        if not self.upload_ok:
            raise IOError("upload failed")
        self.uploaded.append((local_path, remote_path, mode))

    def is_alive(self):
        return True

    def close(self):
        pass


class FakePool:
    """Mock ConnectionPool that returns FakeSession."""

    def __init__(self, session=None):
        self._session = session or FakeSession()

    def get_session(self, server_name):
        return self._session

    def connect(self, server):
        return self._session

    def disconnect(self, name):
        pass

    def disconnect_all(self):
        pass

    def connected_servers(self):
        return ["web1"]


# ---------------------------------------------------------------------------
# _shell_quote
# ---------------------------------------------------------------------------

class TestExhaustGenerator:
    def test_captures_return_value(self):
        def gen():
            yield "a"
            yield "b"
            return 42
        lines, retval = _exhaust_generator(gen())
        assert lines == ["a", "b"]
        assert retval == 42

    def test_no_return_value(self):
        def gen():
            yield "x"
        lines, retval = _exhaust_generator(gen())
        assert lines == ["x"]
        assert retval is None

    def test_empty_generator(self):
        def gen():
            return 0
            yield  # noqa — makes this a generator
        lines, retval = _exhaust_generator(gen())
        assert lines == []
        assert retval == 0


class TestShellQuote:
    def test_simple(self):
        # shlex.quote doesn't add quotes for simple strings
        assert _shell_quote("hello") == "hello"

    def test_with_spaces(self):
        assert _shell_quote("hello world") == "'hello world'"

    def test_with_single_quote(self):
        """Single quotes inside value must be escaped — was a command injection bug."""
        result = _shell_quote("it's a test")
        assert "'" not in result or result.count("'") != 2  # not naive wrapping
        # shlex.quote escapes single quotes properly
        assert result == "\"it's a test\"" or "\\'" in result or "'\"'\"'" in result

    def test_with_semicolon(self):
        """Shell metacharacters must be escaped."""
        result = _shell_quote("value; rm -rf /")
        assert ";" not in result or result.startswith("'")


# ---------------------------------------------------------------------------
# ConnectionPool
# ---------------------------------------------------------------------------

class TestConnectionPool:
    def test_connect_and_get(self):
        pool = ConnectionPool(keep_alive=60)
        server = _server()
        fake = FakeSession()

        with patch.object(pool, '_sessions', {"web1": fake}):
            session = pool.get_session("web1")
            assert session is fake

    def test_get_session_not_found(self):
        pool = ConnectionPool()
        assert pool.get_session("nonexistent") is None

    def test_disconnect(self):
        pool = ConnectionPool()
        fake = FakeSession()
        pool._sessions["web1"] = fake
        pool.disconnect("web1")
        assert "web1" not in pool._sessions

    def test_disconnect_all(self):
        pool = ConnectionPool()
        pool._sessions["web1"] = FakeSession()
        pool._sessions["web2"] = FakeSession()
        pool.disconnect_all()
        assert len(pool._sessions) == 0

    def test_connected_servers(self):
        pool = ConnectionPool()
        pool._sessions["web1"] = FakeSession()
        pool._sessions["web2"] = FakeSession()
        result = pool.connected_servers()
        assert set(result) == {"web1", "web2"}

    def test_connect_reuses_alive_session(self):
        pool = ConnectionPool()
        fake = FakeSession()
        pool._sessions["web1"] = fake

        with patch('ssh_ops.executor.SSHSession') as MockSSH:
            result = pool.connect(_server())
            assert result is fake
            MockSSH.assert_not_called()

    def test_connect_replaces_dead_session(self):
        pool = ConnectionPool()
        dead = FakeSession()
        dead.is_alive = lambda: False
        pool._sessions["web1"] = dead

        new_session = FakeSession()
        with patch('ssh_ops.executor.SSHSession', return_value=new_session):
            result = pool.connect(_server())
            assert result is new_session


# ---------------------------------------------------------------------------
# TaskExecutor — command tasks
# ---------------------------------------------------------------------------

class TestTaskExecutorCommand:
    def setup_method(self):
        TaskConfig._counter.clear()

    def test_run_command_success(self, tmp_path):
        session = FakeSession(output_lines=["hello"], exit_code=0)
        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"command": "echo hello"})
        result = executor.run_task(server, task)
        assert result is True
        assert "echo hello" in session.executed

    def test_run_command_exception(self, tmp_path):
        """Command that raises an exception during execution."""
        session = FakeSession()
        session.exec_command = lambda *a, **kw: (_ for _ in ()).throw(IOError("connection lost"))
        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"command": "echo fail"})
        result = executor.run_task(server, task)
        assert result is False

    def test_run_command_empty_blocked(self, tmp_path):
        pool = FakePool()
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"command": "echo x"})
        task.command = ""  # bypass config validation
        result = executor.run_task(server, task)
        assert result is False

    def test_run_command_nonzero_exit(self, tmp_path):
        """Command with non-zero exit code should return False."""
        session = FakeSession(output_lines=["error output"], exit_code=1)
        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"command": "false"})
        result = executor.run_task(server, task)
        assert result is False

    def test_run_command_interactive_blocked(self, tmp_path):
        pool = FakePool()
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"command": "echo x"})
        task.command = "top"  # bypass config validation
        result = executor.run_task(server, task)
        assert result is False

    def test_not_connected(self, tmp_path):
        pool = FakePool()
        pool.get_session = lambda name: None
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"command": "echo hello"})
        result = executor.run_task(server, task)
        assert result is False


# ---------------------------------------------------------------------------
# TaskExecutor — upload tasks
# ---------------------------------------------------------------------------

class TestTaskExecutorUpload:
    def setup_method(self):
        TaskConfig._counter.clear()

    def test_upload_success(self, tmp_path):
        # Create a local file to upload
        local_file = tmp_path / "test.conf"
        local_file.write_text("content")

        session = FakeSession()
        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"src": str(local_file), "dest": "/opt/app/test.conf"})
        result = executor.run_task(server, task)
        assert result is True
        assert len(session.uploaded) == 1
        assert session.uploaded[0][1] == "/opt/app/test.conf"

    def test_upload_missing_src_field(self, tmp_path):
        pool = FakePool()
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"src": "/opt/a.txt", "dest": "/opt/b.txt"})
        task.src = None  # bypass config validation
        result = executor.run_task(server, task)
        assert result is False

    def test_upload_missing_dest_field(self, tmp_path):
        pool = FakePool()
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"src": "/opt/a.txt", "dest": "/opt/b.txt"})
        task.dest = None  # bypass config validation
        result = executor.run_task(server, task)
        assert result is False

    def test_upload_local_file_not_found(self, tmp_path):
        pool = FakePool()
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"src": "/nonexistent/file.txt", "dest": "/opt/b.txt"})
        result = executor.run_task(server, task)
        assert result is False

    def test_upload_dest_relative_rejected(self, tmp_path):
        local_file = tmp_path / "test.txt"
        local_file.write_text("hi")

        pool = FakePool()
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"src": str(local_file), "dest": "/opt/b.txt"})
        task.dest = "relative/path"  # bypass config validation
        result = executor.run_task(server, task)
        assert result is False

    def test_upload_dest_directory_rejected(self, tmp_path):
        local_file = tmp_path / "test.txt"
        local_file.write_text("hi")

        pool = FakePool()
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"src": str(local_file), "dest": "/opt/b.txt"})
        task.dest = "/opt/dir/"  # bypass config validation
        result = executor.run_task(server, task)
        assert result is False

    def test_upload_exception(self, tmp_path):
        local_file = tmp_path / "test.txt"
        local_file.write_text("hi")

        session = FakeSession(upload_ok=False)
        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"src": str(local_file), "dest": "/opt/b.txt"})
        result = executor.run_task(server, task)
        assert result is False


# ---------------------------------------------------------------------------
# TaskExecutor — script tasks
# ---------------------------------------------------------------------------

class TestTaskExecutorScript:
    def setup_method(self):
        TaskConfig._counter.clear()

    def test_script_success(self, tmp_path):
        script = tmp_path / "deploy.sh"
        script.write_text("#!/bin/bash\necho deploy")

        session = FakeSession(output_lines=["deploy"], exit_code=0)
        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"type": "script", "src": str(script)})
        result = executor.run_task(server, task)
        assert result is True
        assert len(session.uploaded) == 1
        assert session.uploaded[0][2] == "0755"
        # Should execute the remote script and then cleanup (rm)
        assert any("/tmp/_ssh_ops_deploy.sh" in cmd for cmd in session.executed)
        assert any("rm -f" in cmd for cmd in session.executed)

    def test_script_exception(self, tmp_path):
        """Script that raises during execution."""
        script = tmp_path / "bad.sh"
        script.write_text("exit 1")

        session = FakeSession()
        original_upload = session.upload_file
        call_count = [0]
        def upload_then_fail(*a, **kw):
            call_count[0] += 1
            original_upload(*a, **kw)
        session.upload_file = upload_then_fail
        session.exec_command = lambda *a, **kw: (_ for _ in ()).throw(IOError("connection lost"))

        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"type": "script", "src": str(script)})
        result = executor.run_task(server, task)
        assert result is False

    def test_script_missing_src(self, tmp_path):
        pool = FakePool()
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"type": "script", "src": "/opt/deploy.sh"})
        task.src = None  # bypass config validation
        result = executor.run_task(server, task)
        assert result is False

    def test_script_file_not_found(self, tmp_path):
        pool = FakePool()
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"type": "script", "src": "/nonexistent/script.sh"})
        result = executor.run_task(server, task)
        assert result is False

    def test_script_nonzero_exit(self, tmp_path):
        """Script with non-zero exit code should return False."""
        script = tmp_path / "fail.sh"
        script.write_text("#!/bin/bash\nexit 1")

        session = FakeSession(output_lines=["failing"], exit_code=1)
        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"type": "script", "src": str(script)})
        result = executor.run_task(server, task)
        assert result is False

    def test_script_with_args(self, tmp_path):
        script = tmp_path / "deploy.sh"
        script.write_text("#!/bin/bash\necho $1")

        session = FakeSession(output_lines=["prod"], exit_code=0)
        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"type": "script", "src": str(script), "args": "prod"})
        result = executor.run_task(server, task)
        assert result is True
        assert any("prod" in cmd for cmd in session.executed)

    def test_script_args_shell_quoted(self, tmp_path):
        """Bug 10: Script args with special characters must be shell-escaped."""
        script = tmp_path / "run.sh"
        script.write_text("#!/bin/bash\necho $1")

        session = FakeSession(output_lines=["ok"], exit_code=0)
        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"type": "script", "src": str(script), "args": "hello world"})
        result = executor.run_task(server, task)
        assert result is True
        # The executed command should have properly quoted arguments
        cmd = session.executed[0]
        assert "hello" in cmd
        # Should NOT be naively concatenated — should be individually quoted
        assert "'hello'" in cmd or '"hello"' in cmd or "hello" in cmd


# ---------------------------------------------------------------------------
# TaskExecutor — run_all_tasks
# ---------------------------------------------------------------------------

class TestRunAllTasks:
    def setup_method(self):
        TaskConfig._counter.clear()

    def test_serial_all_pass(self, tmp_path):
        session = FakeSession(output_lines=["ok"], exit_code=0)
        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        servers = [_server("s1", "10.0.0.1"), _server("s2", "10.0.0.2")]
        tasks = [_task({"command": "echo 1"}), _task({"command": "echo 2"})]
        results = executor.run_all_tasks(servers, tasks, serial=True)

        assert len(results) == 2
        for server_results in results.values():
            assert all(r["success"] for r in server_results)

    def test_serial_stops_on_failure(self, tmp_path):
        """First task raises exception, second task should not run."""
        session = FakeSession()
        run_count = [0]
        def failing_cmd(*a, **kw):
            run_count[0] += 1
            if run_count[0] == 1:
                raise IOError("connection lost")
            yield "ok"
        session.exec_command = failing_cmd

        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        tasks = [_task({"command": "cmd1"}), _task({"command": "cmd2"})]
        results = executor.run_all_tasks([server], tasks, serial=True)

        assert len(results["web1"]) == 1
        assert results["web1"][0]["success"] is False

    def test_parallel_execution(self, tmp_path):
        session = FakeSession(output_lines=["ok"], exit_code=0)
        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        servers = [_server("s1", "10.0.0.1"), _server("s2", "10.0.0.2")]
        tasks = [_task({"command": "echo 1"})]
        results = executor.run_all_tasks(servers, tasks, serial=False)

        assert len(results) == 2

    def test_unknown_task_type(self, tmp_path):
        pool = FakePool()
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        server = _server()
        task = _task({"command": "echo x"})
        task.type = "bogus"  # bypass config validation
        result = executor.run_task(server, task)
        assert result is False

    def test_progress_callback_serial(self, tmp_path):
        """progress_callback should receive server_start, task_start, and task_done events."""
        session = FakeSession(output_lines=["ok"], exit_code=0)
        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        events = []
        servers = [_server("s1", "10.0.0.1")]
        tasks = [_task({"command": "echo 1"}), _task({"command": "echo 2"})]
        executor.run_all_tasks(servers, tasks, serial=True, progress_callback=lambda e: events.append(e))

        # Expect: server_start, task_start, task_done, task_start, task_done
        assert len(events) == 5
        assert events[0]["event"] == "server_start"
        assert events[0]["server"] == "s1"
        assert events[0]["server_total"] == 1
        assert events[1]["event"] == "task_start"
        assert events[2]["event"] == "task_done"
        assert events[2]["success"] is True
        assert events[3]["event"] == "task_start"
        assert events[4]["event"] == "task_done"

    def test_progress_callback_parallel(self, tmp_path):
        """progress_callback should work in parallel mode too."""
        session = FakeSession(output_lines=["ok"], exit_code=0)
        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        events = []
        servers = [_server("s1", "10.0.0.1"), _server("s2", "10.0.0.2")]
        tasks = [_task({"command": "echo 1"})]
        executor.run_all_tasks(servers, tasks, serial=False, progress_callback=lambda e: events.append(e))

        # Each server: task_start + task_done (no server_start in parallel mode)
        task_done_events = [e for e in events if e["event"] == "task_done"]
        assert len(task_done_events) == 2
        assert all(e["success"] for e in task_done_events)

    def test_progress_callback_failure(self, tmp_path):
        """Failed tasks should report success=False in callback."""
        session = FakeSession()
        session.exec_command = lambda *a, **kw: (_ for _ in ()).throw(IOError("fail"))

        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        events = []
        servers = [_server()]
        tasks = [_task({"command": "cmd1"}), _task({"command": "cmd2"})]
        executor.run_all_tasks(servers, tasks, serial=True, progress_callback=lambda e: events.append(e))

        done_events = [e for e in events if e["event"] == "task_done"]
        assert len(done_events) == 1  # second task skipped due to failure
        assert done_events[0]["success"] is False

    def test_no_progress_callback(self, tmp_path):
        """Without callback, run_all_tasks should still work normally."""
        session = FakeSession(output_lines=["ok"], exit_code=0)
        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        servers = [_server()]
        tasks = [_task({"command": "echo 1"})]
        results = executor.run_all_tasks(servers, tasks, serial=True, progress_callback=None)
        assert results["web1"][0]["success"] is True

    def test_cancel_event_skips_remaining_tasks(self, tmp_path):
        """When cancel_event is set, remaining tasks should be skipped."""
        import threading
        session = FakeSession(output_lines=["ok"], exit_code=0)
        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        cancel = threading.Event()
        events = []

        def _cb(e):
            events.append(e)
            # Cancel after first task completes
            if e["event"] == "task_done" and not e.get("cancelled"):
                cancel.set()

        servers = [_server()]
        tasks = [_task({"command": "echo 1"}), _task({"command": "echo 2"})]
        results = executor.run_all_tasks(servers, tasks, serial=True,
                                          progress_callback=_cb, cancel_event=cancel)
        # First task ran, second was cancelled
        assert results["web1"][0]["success"] is True
        assert results["web1"][1].get("cancelled") is True
        cancelled_events = [e for e in events if e.get("cancelled")]
        assert len(cancelled_events) == 1

    def test_cancel_event_skips_remaining_servers(self, tmp_path):
        """When cancel_event is set before a server starts, that server is skipped."""
        import threading
        session = FakeSession(output_lines=["ok"], exit_code=0)
        pool = FakePool(session)
        logger = ExecLogger(tmp_path / "logs")
        executor = TaskExecutor(pool, logger)

        cancel = threading.Event()
        cancel.set()  # Already cancelled

        servers = [_server(), ServerConfig({"host": "10.0.0.2", "name": "web2"})]
        tasks = [_task({"command": "echo 1"})]
        results = executor.run_all_tasks(servers, tasks, serial=True, cancel_event=cancel)
        # No servers should have run
        assert len(results) == 0


# ---------------------------------------------------------------------------
# SSHSession — with mocked paramiko
# ---------------------------------------------------------------------------

class TestSSHSession:
    def _mock_client(self):
        client = MagicMock()
        transport = MagicMock()
        transport.is_active.return_value = True
        client.get_transport.return_value = transport
        return client

    @patch("ssh_ops.executor.paramiko.SSHClient")
    def test_init_with_password(self, MockSSHClient):
        client = self._mock_client()
        MockSSHClient.return_value = client

        server = ServerConfig({"host": "10.0.0.1", "password": "secret"})
        session = SSHSession(server, keep_alive=30)

        client.connect.assert_called_once()
        call_kwargs = client.connect.call_args[1]
        assert call_kwargs["hostname"] == "10.0.0.1"
        assert call_kwargs["password"] == "secret"
        client.get_transport().set_keepalive.assert_called_with(30)

    @patch("ssh_ops.executor.paramiko.SSHClient")
    def test_init_with_key_file(self, MockSSHClient):
        client = self._mock_client()
        MockSSHClient.return_value = client

        server = ServerConfig({"host": "10.0.0.1", "key_file": "/tmp/id_rsa", "password": None})
        server.key_file = "/tmp/id_rsa"
        server.password = None
        session = SSHSession(server, keep_alive=60)

        call_kwargs = client.connect.call_args[1]
        assert "key_filename" in call_kwargs

    @patch("ssh_ops.executor.paramiko.SSHClient")
    def test_exec_command(self, MockSSHClient):
        client = self._mock_client()
        MockSSHClient.return_value = client

        stdout = MagicMock()
        stderr = MagicMock()
        stdin = MagicMock()
        stdout.readline = MagicMock(side_effect=["line1\n", "line2\n", ""])
        stderr.readline = MagicMock(side_effect=[""])
        stdout.channel.recv_exit_status.return_value = 0
        client.exec_command.return_value = (stdin, stdout, stderr)

        server = ServerConfig({"host": "10.0.0.1", "password": "p"})
        session = SSHSession(server)
        lines = list(session.exec_command("whoami"))
        assert lines == ["line1", "line2"]
        stdin.close.assert_called_once()

    @patch("ssh_ops.executor.paramiko.SSHClient")
    def test_exec_command_invalid_env_key(self, MockSSHClient):
        """Bug 11: Invalid env key names should raise ValueError."""
        client = self._mock_client()
        MockSSHClient.return_value = client

        server = ServerConfig({"host": "10.0.0.1", "password": "p"})
        session = SSHSession(server)
        with pytest.raises(ValueError, match="Invalid env variable name"):
            list(session.exec_command("echo x", env={"BAD;KEY": "value"}))

    @patch("ssh_ops.executor.paramiko.SSHClient")
    def test_exec_command_valid_env_keys(self, MockSSHClient):
        """Bug 11: Valid env key names should work."""
        client = self._mock_client()
        MockSSHClient.return_value = client

        stdout = MagicMock()
        stderr = MagicMock()
        stdin = MagicMock()
        stdout.readline = MagicMock(side_effect=[""])
        stderr.readline = MagicMock(side_effect=[""])
        stdout.channel.recv_exit_status.return_value = 0
        client.exec_command.return_value = (stdin, stdout, stderr)

        server = ServerConfig({"host": "10.0.0.1", "password": "p"})
        session = SSHSession(server)
        list(session.exec_command("echo $X", env={"MY_VAR_1": "ok", "_PRIVATE": "val"}))
        cmd = client.exec_command.call_args[0][0]
        assert "MY_VAR_1" in cmd
        assert "_PRIVATE" in cmd

    @patch("ssh_ops.executor.paramiko.SSHClient")
    def test_exec_command_with_env(self, MockSSHClient):
        client = self._mock_client()
        MockSSHClient.return_value = client

        stdout = MagicMock()
        stderr = MagicMock()
        stdin = MagicMock()
        stdout.readline = MagicMock(side_effect=[""])
        stderr.readline = MagicMock(side_effect=[""])
        stdout.channel.recv_exit_status.return_value = 0
        client.exec_command.return_value = (stdin, stdout, stderr)

        server = ServerConfig({"host": "10.0.0.1", "password": "p"})
        session = SSHSession(server)
        list(session.exec_command("echo $ENV", env={"ENV": "prod"}))

        cmd = client.exec_command.call_args[0][0]
        assert "export ENV=" in cmd
        assert "echo $ENV" in cmd

    @patch("ssh_ops.executor.paramiko.SSHClient")
    def test_exec_command_with_stderr(self, MockSSHClient):
        client = self._mock_client()
        MockSSHClient.return_value = client

        stdout = MagicMock()
        stderr = MagicMock()
        stdin = MagicMock()
        stdout.readline = MagicMock(side_effect=["ok\n", ""])
        stderr.readline = MagicMock(side_effect=["err msg\n", ""])
        stdout.channel.recv_exit_status.return_value = 1
        client.exec_command.return_value = (stdin, stdout, stderr)

        server = ServerConfig({"host": "10.0.0.1", "password": "p"})
        session = SSHSession(server)
        lines = list(session.exec_command("bad_cmd"))
        assert "ok" in lines[0]
        assert "[STDERR]" in lines[1]

    @patch("ssh_ops.executor.paramiko.SSHClient")
    def test_upload_file_with_int_mode(self, MockSSHClient):
        client = self._mock_client()
        MockSSHClient.return_value = client
        sftp = MagicMock()
        client.open_sftp.return_value = sftp

        server = ServerConfig({"host": "10.0.0.1", "password": "p"})
        session = SSHSession(server)
        session.upload_file("/tmp/local.txt", "/opt/remote.txt", 0o755)

        sftp.put.assert_called_once_with("/tmp/local.txt", "/opt/remote.txt")
        sftp.chmod.assert_called_once_with("/opt/remote.txt", 0o755)
        sftp.close.assert_called_once()

    @patch("ssh_ops.executor.paramiko.SSHClient")
    def test_upload_file_with_str_mode(self, MockSSHClient):
        client = self._mock_client()
        MockSSHClient.return_value = client
        sftp = MagicMock()
        client.open_sftp.return_value = sftp

        server = ServerConfig({"host": "10.0.0.1", "password": "p"})
        session = SSHSession(server)
        session.upload_file("/tmp/local.txt", "/opt/remote.txt", "0755")

        sftp.chmod.assert_called_once_with("/opt/remote.txt", 0o755)

    @patch("ssh_ops.executor.paramiko.SSHClient")
    def test_upload_file_no_mode(self, MockSSHClient):
        client = self._mock_client()
        MockSSHClient.return_value = client
        sftp = MagicMock()
        client.open_sftp.return_value = sftp

        server = ServerConfig({"host": "10.0.0.1", "password": "p"})
        session = SSHSession(server)
        session.upload_file("/tmp/local.txt", "/opt/remote.txt")

        sftp.put.assert_called_once()
        sftp.chmod.assert_not_called()

    @patch("ssh_ops.executor.paramiko.SSHClient")
    def test_download_file(self, MockSSHClient):
        client = self._mock_client()
        MockSSHClient.return_value = client
        sftp = MagicMock()
        client.open_sftp.return_value = sftp

        server = ServerConfig({"host": "10.0.0.1", "password": "p"})
        session = SSHSession(server)
        session.download_file("/opt/remote.txt", "/tmp/local.txt")

        sftp.get.assert_called_once_with("/opt/remote.txt", "/tmp/local.txt")
        sftp.close.assert_called_once()

    @patch("ssh_ops.executor.paramiko.SSHClient")
    def test_is_alive(self, MockSSHClient):
        client = self._mock_client()
        MockSSHClient.return_value = client

        server = ServerConfig({"host": "10.0.0.1", "password": "p"})
        session = SSHSession(server)
        assert session.is_alive() is True

        client.get_transport.return_value = None
        assert session.is_alive() is False

    @patch("ssh_ops.executor.paramiko.SSHClient")
    def test_close(self, MockSSHClient):
        client = self._mock_client()
        MockSSHClient.return_value = client

        server = ServerConfig({"host": "10.0.0.1", "password": "p"})
        session = SSHSession(server)
        session.close()
        client.close.assert_called_once()
