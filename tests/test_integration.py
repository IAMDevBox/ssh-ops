"""Integration tests using Docker SSH nodes.

Requires: docker/start.sh running (3 nodes on ports 2201-2203).
Skip automatically if nodes are not reachable.
"""

import socket
import pytest
from pathlib import Path

from ssh_ops.config import AppConfig, TaskConfig, ServerConfig
from ssh_ops.executor import ConnectionPool, TaskExecutor, SSHSession, _exhaust_generator
from ssh_ops.logger import ExecLogger


def _docker_available():
    """Check if Docker SSH node1 is reachable."""
    try:
        s = socket.create_connection(("127.0.0.1", 2201), timeout=2)
        s.close()
        return True
    except (ConnectionRefusedError, OSError):
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available(),
    reason="Docker SSH nodes not running (start with docker/start.sh)"
)


def _docker_server(name="node1", port=2201):
    return ServerConfig({
        "host": "127.0.0.1", "port": port,
        "name": name, "username": "testuser", "password": "test123",
    })


@pytest.fixture
def docker_servers():
    return [
        _docker_server("node1", 2201),
        _docker_server("node2", 2202),
        _docker_server("node3", 2203),
    ]


@pytest.fixture
def pool():
    p = ConnectionPool(keep_alive=60)
    yield p
    p.disconnect_all()


@pytest.fixture
def logger(tmp_path):
    return ExecLogger(tmp_path / "logs")


# ---------------------------------------------------------------------------
# SSHSession — real paramiko connections
# ---------------------------------------------------------------------------

class TestSSHSessionReal:
    def test_connect_and_alive(self):
        server = _docker_server()
        session = SSHSession(server)
        assert session.is_alive()
        session.close()
        assert not session.is_alive()

    def test_exec_command(self):
        server = _docker_server()
        session = SSHSession(server)
        lines, code = _exhaust_generator(session.exec_command("hostname"))
        assert code == 0
        assert len(lines) >= 1
        session.close()

    def test_exec_command_exit_code(self):
        server = _docker_server()
        session = SSHSession(server)
        lines, code = _exhaust_generator(session.exec_command("exit 42"))
        assert code == 42
        session.close()

    def test_exec_command_with_env(self):
        server = _docker_server()
        session = SSHSession(server)
        lines, code = _exhaust_generator(
            session.exec_command("echo $MY_VAR", env={"MY_VAR": "hello123"})
        )
        assert code == 0
        assert any("hello123" in line for line in lines)
        session.close()

    def test_exec_command_multiline_output(self):
        server = _docker_server()
        session = SSHSession(server)
        lines, code = _exhaust_generator(session.exec_command("echo line1; echo line2; echo line3"))
        assert code == 0
        assert len(lines) >= 3
        session.close()

    def test_exec_command_stderr(self):
        server = _docker_server()
        session = SSHSession(server)
        lines, code = _exhaust_generator(session.exec_command("echo err >&2; echo out"))
        # stderr is captured too
        assert code == 0
        session.close()

    def test_upload_file(self, tmp_path):
        server = _docker_server()
        session = SSHSession(server)
        local = tmp_path / "upload_test.txt"
        local.write_text("integration test content")
        remote = "/tmp/_ssh_ops_integration_test.txt"
        session.upload_file(str(local), remote)
        lines, code = _exhaust_generator(session.exec_command(f"cat {remote}"))
        assert code == 0
        assert any("integration test content" in line for line in lines)
        # Cleanup
        _exhaust_generator(session.exec_command(f"rm -f {remote}"))
        session.close()

    def test_upload_file_with_mode(self, tmp_path):
        server = _docker_server()
        session = SSHSession(server)
        local = tmp_path / "script_test.sh"
        local.write_text("#!/bin/bash\necho hello")
        remote = "/tmp/_ssh_ops_mode_test.sh"
        session.upload_file(str(local), remote, "0755")
        lines, code = _exhaust_generator(session.exec_command(f"stat -c %a {remote}"))
        assert code == 0
        assert any("755" in line for line in lines)
        _exhaust_generator(session.exec_command(f"rm -f {remote}"))
        session.close()

    def test_download_file(self, tmp_path):
        server = _docker_server()
        session = SSHSession(server)
        # Create a remote file first
        _exhaust_generator(session.exec_command("echo download_test > /tmp/_ssh_ops_dl_test.txt"))
        local = tmp_path / "downloaded.txt"
        session.download_file("/tmp/_ssh_ops_dl_test.txt", str(local))
        assert local.exists()
        assert "download_test" in local.read_text()
        _exhaust_generator(session.exec_command("rm -f /tmp/_ssh_ops_dl_test.txt"))
        session.close()


# ---------------------------------------------------------------------------
# ConnectionPool — real connections
# ---------------------------------------------------------------------------

class TestConnectionPoolReal:
    def test_connect_disconnect(self, docker_servers, pool):
        for s in docker_servers:
            pool.connect(s)
        assert len(pool.connected_servers()) == 3
        for s in docker_servers:
            assert pool.get_session(s.name) is not None
            assert pool.get_session(s.name).is_alive()
        pool.disconnect("node2")
        assert len(pool.connected_servers()) == 2
        pool.disconnect_all()
        assert len(pool.connected_servers()) == 0

    def test_get_session_not_connected(self, pool):
        assert pool.get_session("nonexistent") is None


# ---------------------------------------------------------------------------
# TaskExecutor — real task execution
# ---------------------------------------------------------------------------

class TestTaskExecutorReal:
    def setup_method(self):
        TaskConfig._counter.clear()

    def test_command_task(self, docker_servers, pool, logger):
        pool.connect(docker_servers[0])
        executor = TaskExecutor(pool, logger)
        task = TaskConfig({"command": "whoami"})
        result = executor.run_task(docker_servers[0], task)
        assert result is True

    def test_command_task_failure(self, docker_servers, pool, logger):
        pool.connect(docker_servers[0])
        executor = TaskExecutor(pool, logger)
        task = TaskConfig({"command": "exit 1"})
        result = executor.run_task(docker_servers[0], task)
        assert result is False

    def test_upload_task(self, docker_servers, pool, logger, tmp_path):
        pool.connect(docker_servers[0])
        executor = TaskExecutor(pool, logger)
        local = tmp_path / "task_upload.txt"
        local.write_text("task upload test")
        task = TaskConfig({"src": str(local), "dest": "/tmp/_ssh_ops_task_upload.txt"})
        result = executor.run_task(docker_servers[0], task)
        assert result is True
        # Verify
        session = pool.get_session("node1")
        lines, _ = _exhaust_generator(session.exec_command("cat /tmp/_ssh_ops_task_upload.txt"))
        assert any("task upload test" in l for l in lines)
        _exhaust_generator(session.exec_command("rm -f /tmp/_ssh_ops_task_upload.txt"))

    def test_script_task(self, docker_servers, pool, logger, tmp_path):
        pool.connect(docker_servers[0])
        executor = TaskExecutor(pool, logger)
        script = tmp_path / "test_script.sh"
        script.write_text("#!/bin/bash\necho script_output_$$")
        task = TaskConfig({"type": "script", "src": str(script)})
        result = executor.run_task(docker_servers[0], task)
        assert result is True

    def test_run_all_tasks_serial(self, docker_servers, pool, logger):
        for s in docker_servers:
            pool.connect(s)
        executor = TaskExecutor(pool, logger)
        tasks = [TaskConfig({"command": "hostname"}), TaskConfig({"command": "whoami"})]
        results = executor.run_all_tasks(docker_servers, tasks, serial=True)
        assert len(results) == 3
        for server_results in results.values():
            assert all(r["success"] for r in server_results)

    def test_run_all_tasks_parallel(self, docker_servers, pool, logger):
        for s in docker_servers:
            pool.connect(s)
        executor = TaskExecutor(pool, logger)
        tasks = [TaskConfig({"command": "hostname"})]
        results = executor.run_all_tasks(docker_servers, tasks, serial=False)
        assert len(results) == 3
        for server_results in results.values():
            assert all(r["success"] for r in server_results)

    def test_progress_callback_real(self, docker_servers, pool, logger):
        pool.connect(docker_servers[0])
        executor = TaskExecutor(pool, logger)
        events = []
        tasks = [TaskConfig({"command": "hostname"}), TaskConfig({"command": "whoami"})]
        results = executor.run_all_tasks(
            [docker_servers[0]], tasks, serial=True,
            progress_callback=lambda e: events.append(e)
        )
        assert len(events) == 5  # server_start + 2*(task_start + task_done)
        assert events[0]["event"] == "server_start"
        assert all(e["success"] for e in events if e["event"] == "task_done")

    def test_run_all_stops_on_failure(self, docker_servers, pool, logger):
        pool.connect(docker_servers[0])
        executor = TaskExecutor(pool, logger)
        tasks = [
            TaskConfig({"command": "exit 1"}),
            TaskConfig({"command": "echo should_not_run"}),
        ]
        results = executor.run_all_tasks([docker_servers[0]], tasks, serial=True)
        # First task fails, second should be skipped
        server_results = list(results.values())[0]
        assert len(server_results) == 1
        assert server_results[0]["success"] is False

    def test_command_with_timeout(self, docker_servers, pool, logger):
        pool.connect(docker_servers[0])
        executor = TaskExecutor(pool, logger)
        task = TaskConfig({"command": "echo fast", "timeout": 10})
        result = executor.run_task(docker_servers[0], task)
        assert result is True


# ---------------------------------------------------------------------------
# CLI dry-run with docker config
# ---------------------------------------------------------------------------

class TestCLIDryRunDocker:
    def test_dry_run_docker_config(self, capsys):
        from unittest.mock import patch
        TaskConfig._counter.clear()
        with patch("sys.argv", ["ssh-ops", "-c", "config/docker-test.yml", "run", "--dry-run"]):
            from ssh_ops.cli import main
            main()
        out = capsys.readouterr().out
        assert "DRY RUN PREVIEW" in out
        assert "node1" in out
        assert "node2" in out
        assert "node3" in out


# ---------------------------------------------------------------------------
# Web API with docker config
# ---------------------------------------------------------------------------

class TestWebAPIDryRunDocker:
    def test_dry_run_docker(self):
        from fastapi.testclient import TestClient
        from ssh_ops.server import create_app
        TaskConfig._counter.clear()
        config = AppConfig("config/docker-test.yml")
        logger = ExecLogger(Path("./logs"))
        app = create_app(config, logger)
        c = TestClient(app)
        resp = c.post("/api/dry-run", json={"servers": ["node1", "node2", "node3"]})
        data = resp.json()
        assert len(data["servers"]) == 3
        assert len(data["tasks"]) == len(config.tasks)
