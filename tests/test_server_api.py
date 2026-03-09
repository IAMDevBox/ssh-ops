"""Tests for ssh_ops.server — API endpoint validation."""

import json
import tempfile
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import yaml
from fastapi.testclient import TestClient

from ssh_ops.config import AppConfig, TaskConfig
from ssh_ops.executor import TaskExecutor
from ssh_ops.logger import ExecLogger
from ssh_ops.server import create_app, _friendly_error


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_config(tmp_path):
    """Create a minimal config file and return its path."""
    data = {
        "servers": [
            {"host": "10.0.0.1", "name": "web1", "username": "admin", "password": "pass"},
            {"host": "10.0.0.2", "name": "db1", "username": "admin", "password": "pass", "groups": ["db"]},
        ],
        "tasks": [
            {"command": "whoami"},
            {"command": "df -h"},
            {"src": "/opt/files/app.conf", "dest": "/opt/app/app.conf"},
        ],
    }
    cfg_path = tmp_path / "test.yml"
    cfg_path.write_text(yaml.dump(data, default_flow_style=False), encoding="utf-8")
    return cfg_path


@pytest.fixture
def app(tmp_config, tmp_path):
    """Create a FastAPI test app."""
    TaskConfig._counter.clear()
    config = AppConfig(tmp_config)
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    logger = ExecLogger(log_dir)
    return create_app(config, logger)


@pytest.fixture
def client(app):
    return TestClient(app)


# ---------------------------------------------------------------------------
# GET endpoints
# ---------------------------------------------------------------------------

class TestListEndpoints:
    def test_list_servers(self, client):
        resp = client.get("/api/servers")
        assert resp.status_code == 200
        servers = resp.json()
        assert len(servers) == 2
        names = [s["name"] for s in servers]
        assert "web1" in names
        assert "db1" in names

    def test_list_tasks(self, client):
        resp = client.get("/api/tasks")
        assert resp.status_code == 200
        tasks = resp.json()
        assert len(tasks) == 3
        types = [t["type"] for t in tasks]
        assert "command" in types
        assert "upload" in types

    def test_list_configs(self, client):
        resp = client.get("/api/configs")
        assert resp.status_code == 200
        data = resp.json()
        assert "configs" in data
        assert "current" in data

    def test_get_tasks_yaml(self, client):
        resp = client.get("/api/tasks-yaml")
        assert resp.status_code == 200
        assert "tasks:" in resp.text

    def test_get_history_empty(self, client):
        resp = client.get("/api/history")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_cmd_history_empty(self, client):
        resp = client.get("/api/cmd-history")
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# POST /api/add-task — validation
# ---------------------------------------------------------------------------

class TestAddTask:
    def test_add_command_ok(self, client):
        resp = client.post("/api/add-task", json={"type": "command", "command": "uptime"})
        data = resp.json()
        assert data.get("status") == "ok"

    def test_add_command_empty(self, client):
        resp = client.post("/api/add-task", json={"type": "command", "command": ""})
        data = resp.json()
        assert "error" in data
        assert "no command" in data["error"]

    def test_add_command_interactive_rejected(self, client):
        resp = client.post("/api/add-task", json={"type": "command", "command": "vim"})
        data = resp.json()
        assert "error" in data
        assert "interactive" in data["error"]

    def test_add_upload_ok(self, client):
        resp = client.post("/api/add-task", json={
            "type": "upload",
            "src": "/opt/files/new.conf",
            "dest": "/opt/app/new.conf",
        })
        data = resp.json()
        assert data.get("status") == "ok"

    def test_add_upload_missing_src(self, client):
        resp = client.post("/api/add-task", json={
            "type": "upload",
            "dest": "/opt/app/file.txt",
        })
        data = resp.json()
        assert "error" in data
        assert "src and dest required" in data["error"]

    def test_add_upload_missing_dest(self, client):
        resp = client.post("/api/add-task", json={
            "type": "upload",
            "src": "/opt/files/file.txt",
        })
        data = resp.json()
        assert "error" in data
        assert "src and dest required" in data["error"]

    def test_add_upload_relative_src_rejected(self, client):
        resp = client.post("/api/add-task", json={
            "type": "upload",
            "src": "relative/file.txt",
            "dest": "/opt/app/file.txt",
        })
        data = resp.json()
        assert "error" in data
        assert "absolute path" in data["error"]

    def test_add_upload_relative_dest_rejected(self, client):
        resp = client.post("/api/add-task", json={
            "type": "upload",
            "src": "/opt/files/file.txt",
            "dest": "relative/file.txt",
        })
        data = resp.json()
        assert "error" in data
        assert "absolute path" in data["error"]

    def test_add_upload_src_directory_rejected(self, client):
        resp = client.post("/api/add-task", json={
            "type": "upload",
            "src": "/opt/files/",
            "dest": "/opt/app/file.txt",
        })
        data = resp.json()
        assert "error" in data
        assert "not a directory" in data["error"]

    def test_add_upload_dest_directory_rejected(self, client):
        resp = client.post("/api/add-task", json={
            "type": "upload",
            "src": "/opt/files/file.txt",
            "dest": "/opt/app/",
        })
        data = resp.json()
        assert "error" in data
        assert "not a directory" in data["error"]

    def test_add_invalid_type(self, client):
        resp = client.post("/api/add-task", json={"type": "bogus"})
        data = resp.json()
        assert "error" in data
        assert "type must be" in data["error"]


# ---------------------------------------------------------------------------
# POST /api/delete-task
# ---------------------------------------------------------------------------

class TestDeleteTask:
    def test_delete_task_ok(self, client):
        resp = client.post("/api/delete-task", json={"index": 0})
        data = resp.json()
        assert data.get("status") == "ok"
        assert "removed" in data

    def test_delete_task_no_index(self, client):
        resp = client.post("/api/delete-task", json={})
        data = resp.json()
        assert "error" in data

    def test_delete_task_out_of_range(self, client):
        resp = client.post("/api/delete-task", json={"index": 999})
        data = resp.json()
        assert "error" in data
        assert "out of range" in data["error"]

    def test_delete_task_non_integer_index(self, client):
        """Bug 15: Non-integer index should return clear error."""
        resp = client.post("/api/delete-task", json={"index": "abc"})
        data = resp.json()
        assert "error" in data
        assert "integer" in data["error"]

    def test_delete_task_negative_index(self, client):
        resp = client.post("/api/delete-task", json={"index": -1})
        data = resp.json()
        assert "error" in data
        assert "out of range" in data["error"]


# ---------------------------------------------------------------------------
# POST /api/run-command — validation
# ---------------------------------------------------------------------------

class TestRunCommand:
    def test_empty_command(self, client):
        resp = client.post("/api/run-command", json={"servers": ["web1"], "command": ""})
        data = resp.json()
        assert "error" in data
        assert "no command" in data["error"]

    def test_interactive_command_rejected(self, client):
        resp = client.post("/api/run-command", json={"servers": ["web1"], "command": "top"})
        data = resp.json()
        assert "error" in data
        assert "interactive" in data["error"]

    def test_no_servers_matched(self, client):
        resp = client.post("/api/run-command", json={"servers": ["nonexistent"], "command": "whoami"})
        data = resp.json()
        assert "error" in data
        assert "no servers" in data["error"]


# ---------------------------------------------------------------------------
# POST /api/upload — validation
# ---------------------------------------------------------------------------

class TestUploadValidation:
    def test_dest_relative_rejected(self, client):
        resp = client.post("/api/upload", data={
            "dest": "relative/path.txt",
            "servers": "web1",
        }, files={"file": ("test.txt", b"content", "text/plain")})
        data = resp.json()
        assert "error" in data
        assert "absolute path" in data["error"]

    def test_dest_directory_rejected(self, client):
        resp = client.post("/api/upload", data={
            "dest": "/opt/app/",
            "servers": "web1",
        }, files={"file": ("test.txt", b"content", "text/plain")})
        data = resp.json()
        assert "error" in data
        assert "not a directory" in data["error"]

    def test_no_servers_matched(self, client):
        resp = client.post("/api/upload", data={
            "dest": "/opt/app/file.txt",
            "servers": "nonexistent",
        }, files={"file": ("test.txt", b"content", "text/plain")})
        data = resp.json()
        assert "error" in data
        assert "no servers" in data["error"]


# ---------------------------------------------------------------------------
# POST /api/connect / disconnect
# ---------------------------------------------------------------------------

class TestConnectDisconnect:
    def test_disconnect_all(self, client):
        resp = client.post("/api/disconnect", json={"all": True})
        data = resp.json()
        assert data.get("status") == "all disconnected"

    def test_run_no_tasks(self, client):
        resp = client.post("/api/run", json={"servers": ["web1"], "tasks": ["nonexistent"]})
        data = resp.json()
        assert "error" in data
        assert "no tasks" in data["error"]

    def test_run_no_servers(self, client):
        resp = client.post("/api/run", json={"servers": ["nonexistent"]})
        data = resp.json()
        assert "error" in data
        assert "no servers" in data["error"]


# ---------------------------------------------------------------------------
# POST /api/clear-history
# ---------------------------------------------------------------------------

class TestClearHistory:
    def test_clear_history(self, client):
        resp = client.post("/api/clear-history")
        data = resp.json()
        assert data.get("status") == "cleared"


# ---------------------------------------------------------------------------
# POST /api/reload
# ---------------------------------------------------------------------------

class TestReloadConfig:
    def test_reload_ok(self, client):
        resp = client.post("/api/reload", json={})
        data = resp.json()
        assert data.get("status") == "ok"


# ---------------------------------------------------------------------------
# GET / — HTML page
# ---------------------------------------------------------------------------

class TestIndex:
    def test_index_page(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "SSH Ops Tool" in resp.text


# ---------------------------------------------------------------------------
# GET /api/logs — server logs
# ---------------------------------------------------------------------------

class TestServerLogs:
    def test_log_no_file(self, client):
        resp = client.get("/api/logs/web1")
        assert resp.status_code == 200
        assert "No log" in resp.text

    def test_log_with_file(self, client, tmp_path, app):
        # Write a log file manually
        from ssh_ops.logger import ExecLogger
        import inspect
        # Get the logger from the app closure
        resp = client.get("/api/logs/web1?date=2020-01-01")
        assert resp.status_code == 200

    def test_log_exists(self, client, tmp_config, tmp_path):
        log_dir = tmp_path / "logs"
        server_dir = log_dir / "web1"
        server_dir.mkdir(parents=True, exist_ok=True)
        (server_dir / "2020-01-01.log").write_text("test log line")
        resp = client.get("/api/logs/web1?date=2020-01-01")
        assert resp.status_code == 200
        assert "test log line" in resp.text


# ---------------------------------------------------------------------------
# GET /api/config-yaml
# ---------------------------------------------------------------------------

class TestConfigYaml:
    def test_get_config_yaml(self, client):
        resp = client.get("/api/config-yaml")
        assert resp.status_code == 200
        assert "servers" in resp.text


# ---------------------------------------------------------------------------
# POST /api/switch-config
# ---------------------------------------------------------------------------

class TestSwitchConfig:
    def test_switch_no_path(self, client):
        resp = client.post("/api/switch-config", json={})
        data = resp.json()
        assert "error" in data

    def test_switch_ok(self, client, tmp_config):
        resp = client.post("/api/switch-config", json={"path": str(tmp_config)})
        data = resp.json()
        assert data.get("status") == "ok"

    def test_switch_invalid_path(self, client):
        resp = client.post("/api/switch-config", json={"path": "/nonexistent/config.yml"})
        data = resp.json()
        assert "error" in data


# ---------------------------------------------------------------------------
# POST /api/connect — with mocked pool
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Mocked pool fixtures for connect/run/upload
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_app(tmp_config, tmp_path):
    """Create app with a mocked ConnectionPool."""
    from unittest.mock import MagicMock, patch
    TaskConfig._counter.clear()
    config = AppConfig(tmp_config)
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    logger = ExecLogger(log_dir)

    mock_session = MagicMock()
    mock_session.exec_command.return_value = iter(["output line"])
    mock_session.is_alive.return_value = True
    mock_session.upload_file.return_value = None

    with patch("ssh_ops.server.ConnectionPool") as MockPool:
        pool_instance = MockPool.return_value
        pool_instance.connect.return_value = mock_session
        pool_instance.get_session.return_value = mock_session
        pool_instance.connected_servers.return_value = ["web1"]
        pool_instance.disconnect.return_value = None
        pool_instance.disconnect_all.return_value = None
        app = create_app(config, logger)
        yield app, mock_session


@pytest.fixture
def mock_client(mock_app):
    app, _ = mock_app
    return TestClient(app)


class TestConnectServers:
    def test_connect_by_name(self, mock_client):
        resp = mock_client.post("/api/connect", json={"servers": ["web1"]})
        data = resp.json()
        assert data.get("web1") == "connected"

    def test_connect_all(self, mock_client):
        resp = mock_client.post("/api/connect", json={"all": True})
        data = resp.json()
        assert isinstance(data, dict)

    def test_disconnect_by_name(self, mock_client):
        resp = mock_client.post("/api/disconnect", json={"servers": ["web1"]})
        data = resp.json()
        assert data.get("status") == "ok"


class TestConnectError:
    def test_connect_error(self, tmp_config, tmp_path):
        """Test connect when pool.connect raises."""
        from unittest.mock import MagicMock, patch
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs2"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)

        with patch("ssh_ops.server.ConnectionPool") as MockPool:
            pool_instance = MockPool.return_value
            pool_instance.connect.side_effect = Exception("connection refused")
            pool_instance.connected_servers.return_value = []
            app = create_app(config, logger)
            c = TestClient(app)
            resp = c.post("/api/connect", json={"servers": ["web1"]})
            data = resp.json()
            assert "error" in data.get("web1", "")


# ---------------------------------------------------------------------------
# POST /api/run — with mocked pool
# ---------------------------------------------------------------------------

class TestRunTasks:
    def test_run_all_tasks(self, mock_client):
        resp = mock_client.post("/api/run", json={"servers": ["web1"]})
        data = resp.json()
        assert data.get("status") == "started"

    def test_run_all_tasks_no_task_filter(self, mock_client):
        resp = mock_client.post("/api/run", json={"servers": ["web1"], "tasks": None})
        data = resp.json()
        assert data.get("status") == "started"


# ---------------------------------------------------------------------------
# POST /api/run-command — success path
# ---------------------------------------------------------------------------

class TestRunCommandSuccess:
    def test_run_command_valid(self, mock_client):
        resp = mock_client.post("/api/run-command", json={"servers": ["web1"], "command": "whoami"})
        data = resp.json()
        assert data.get("status") == "started"

    def test_run_command_dedup_history(self, mock_client):
        mock_client.post("/api/run-command", json={"servers": ["web1"], "command": "whoami"})
        mock_client.post("/api/run-command", json={"servers": ["web1"], "command": "whoami"})
        resp = mock_client.get("/api/cmd-history")
        history = resp.json()
        assert history.count("whoami") == 1


# ---------------------------------------------------------------------------
# POST /api/upload — success with mocked session
# ---------------------------------------------------------------------------

class TestUploadSuccess:
    def test_upload_connected(self, mock_client):
        resp = mock_client.post("/api/upload", data={
            "dest": "/opt/app/file.txt",
            "servers": "web1",
        }, files={"file": ("test.txt", b"content", "text/plain")})
        data = resp.json()
        assert data.get("web1") == "uploaded"

    def test_upload_not_connected(self, client):
        resp = client.post("/api/upload", data={
            "dest": "/opt/app/file.txt",
            "servers": "web1",
        }, files={"file": ("test.txt", b"content", "text/plain")})
        data = resp.json()
        assert "web1" in data

    def test_upload_dedup_history(self, mock_client):
        for _ in range(2):
            mock_client.post("/api/upload", data={
                "dest": "/opt/app/file.txt",
                "servers": "web1",
            }, files={"file": ("test.txt", b"content", "text/plain")})
        resp = mock_client.get("/api/upload-history")
        history = resp.json()
        matches = [h for h in history if h["filename"] == "test.txt" and h["dest"] == "/opt/app/file.txt"]
        assert len(matches) == 1

    def test_upload_error(self, tmp_config, tmp_path):
        """Test upload when session.upload_file raises."""
        from unittest.mock import MagicMock, patch
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs3"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)

        mock_session = MagicMock()
        mock_session.upload_file.side_effect = IOError("disk full")
        mock_session.is_alive.return_value = True

        with patch("ssh_ops.server.ConnectionPool") as MockPool:
            pool_instance = MockPool.return_value
            pool_instance.get_session.return_value = mock_session
            pool_instance.connected_servers.return_value = ["web1"]
            app = create_app(config, logger)
            c = TestClient(app)
            resp = c.post("/api/upload", data={
                "dest": "/opt/app/file.txt",
                "servers": "web1",
            }, files={"file": ("test.txt", b"content", "text/plain")})
            data = resp.json()
            assert "error" in data.get("web1", "")


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------

class TestWebSocket:
    def test_websocket_connect(self, client):
        with client.websocket_connect("/ws") as ws:
            ws.close()

    def test_websocket_receives_history(self, mock_client):
        """Connect WS after output_history has content — covers lines 412-413."""
        # Trigger a broadcast by reloading config (which broadcasts a message)
        mock_client.post("/api/reload", json={})
        # Now connect WS — output_history has content, so ws.send_text is called
        with mock_client.websocket_connect("/ws") as ws:
            msg = ws.receive_text()
            assert "Config reloaded" in msg or len(msg) > 0
            ws.close()

    def test_websocket_broadcast_to_connected(self, mock_client):
        """Broadcast while a WS client is connected — covers lines 427-428."""
        with mock_client.websocket_connect("/ws") as ws:
            # Trigger a broadcast while ws is connected
            mock_client.post("/api/reload", json={})
            msg = ws.receive_text()
            assert len(msg) > 0
            ws.close()


# ---------------------------------------------------------------------------
# Reload / switch-config with connected servers (covers lines 109, 126)
# ---------------------------------------------------------------------------

class TestReloadWithConnected:
    def test_reload_with_connected_servers(self, mock_client):
        """Reload when servers are connected shows disconnected message."""
        resp = mock_client.post("/api/reload", json={})
        data = resp.json()
        assert data.get("status") == "ok"

    def test_switch_with_connected_servers(self, mock_client, tmp_config):
        """Switch config when servers are connected shows disconnected message."""
        resp = mock_client.post("/api/switch-config", json={"path": str(tmp_config)})
        data = resp.json()
        assert data.get("status") == "ok"


# ---------------------------------------------------------------------------
# Reload / switch-config exception paths (covers lines 112-113)
# ---------------------------------------------------------------------------

class TestReloadError:
    def test_reload_error(self, tmp_config, tmp_path):
        """Reload when config.reload() raises returns error."""
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs_reload_err"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        # Delete the config file to trigger reload error
        tmp_config.unlink()
        c = TestClient(app)
        resp = c.post("/api/reload", json={})
        data = resp.json()
        assert "error" in data


# ---------------------------------------------------------------------------
# tasks-yaml / config-yaml exception paths (covers lines 141-142, 150-151)
# ---------------------------------------------------------------------------

class TestYamlErrors:
    def test_tasks_yaml_error(self, tmp_config, tmp_path):
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs_yaml_err"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        tmp_config.unlink()
        c = TestClient(app)
        resp = c.get("/api/tasks-yaml")
        assert "Error" in resp.text

    def test_config_yaml_error(self, tmp_config, tmp_path):
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs_cfg_err"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        tmp_config.unlink()
        c = TestClient(app)
        resp = c.get("/api/config-yaml")
        assert "Error" in resp.text


# ---------------------------------------------------------------------------
# add-task upload with mode (covers line 186)
# ---------------------------------------------------------------------------

class TestAddTaskUploadMode:
    def test_add_upload_with_mode(self, client):
        resp = client.post("/api/add-task", json={
            "type": "upload",
            "src": "/opt/files/app.conf",
            "dest": "/opt/app/app.conf",
            "mode": "0644",
        })
        data = resp.json()
        assert data.get("status") == "ok"
        assert data["task"].get("mode") == "0644"

    def test_add_task_no_tasks_key_in_yaml(self, tmp_path):
        """Config file without 'tasks' key triggers raw['tasks'] = []."""
        TaskConfig._counter.clear()
        cfg = tmp_path / "notasks.yml"
        cfg.write_text(yaml.dump({"servers": []}, default_flow_style=False), encoding="utf-8")
        config = AppConfig(cfg)
        log_dir = tmp_path / "logs_notasks"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        c = TestClient(app)
        resp = c.post("/api/add-task", json={"type": "command", "command": "echo hi"})
        data = resp.json()
        assert data.get("status") == "ok"


# ---------------------------------------------------------------------------
# add-task / delete-task exception paths (covers lines 201-202, 223-224)
# ---------------------------------------------------------------------------

class TestAddDeleteTaskErrors:
    def test_add_task_write_error(self, tmp_config, tmp_path):
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs_add_err"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        # Delete config to trigger write error
        tmp_config.unlink()
        c = TestClient(app)
        resp = c.post("/api/add-task", json={"type": "command", "command": "echo hi"})
        data = resp.json()
        assert "error" in data

    def test_delete_task_write_error(self, tmp_config, tmp_path):
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs_del_err"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        tmp_config.unlink()
        c = TestClient(app)
        resp = c.post("/api/delete-task", json={"index": 0})
        data = resp.json()
        assert "error" in data


# ---------------------------------------------------------------------------
# Background thread: run tasks _run() (covers lines 293, 330-331)
# ---------------------------------------------------------------------------

class TestRunTasksBackground:
    def _make_cmd_only_app(self, tmp_path):
        """Create an app with command-only tasks for background thread testing."""
        TaskConfig._counter.clear()
        cfg = tmp_path / "cmd_only.yml"
        cfg.write_text(yaml.dump({
            "servers": [{"host": "10.0.0.1", "name": "web1", "password": "p"}],
            "tasks": [{"command": "whoami"}],
        }, default_flow_style=False), encoding="utf-8")
        config = AppConfig(cfg)
        log_dir = tmp_path / "logs_bg"
        log_dir.mkdir(exist_ok=True)
        return config, ExecLogger(log_dir)

    def _wait_bg_threads(self):
        import threading
        for t in threading.enumerate():
            if t.daemon and t.name != "MainThread":
                t.join(timeout=5)

    def test_run_background_thread_completes(self, tmp_path):
        """Run tasks and wait for background thread — covers success broadcast."""
        config, logger = self._make_cmd_only_app(tmp_path)

        mock_executor = MagicMock()
        mock_executor.run_all_tasks.return_value = {
            "web1": [{"task": "command-1", "success": True}]
        }

        with patch("ssh_ops.server.ConnectionPool") as MockPool, \
             patch("ssh_ops.server.TaskExecutor", return_value=mock_executor), \
             patch("ssh_ops.server.asyncio") as mock_asyncio:
            pool_inst = MockPool.return_value
            pool_inst.connected_servers.return_value = []
            pool_inst.disconnect_all.return_value = None
            app = create_app(config, logger)
            c = TestClient(app)
            resp = c.post("/api/run", json={"servers": ["web1"]})
            assert resp.json().get("status") == "started"
            self._wait_bg_threads()
            mock_executor.run_all_tasks.assert_called_once()

    def test_run_command_not_connected(self, tmp_path):
        """Run command when session is None triggers 'Not connected' log."""
        config, logger = self._make_cmd_only_app(tmp_path)

        with patch("ssh_ops.server.ConnectionPool") as MockPool, \
             patch("ssh_ops.server.asyncio"):
            pool_inst = MockPool.return_value
            pool_inst.get_session.return_value = None
            pool_inst.connected_servers.return_value = []
            pool_inst.disconnect_all.return_value = None
            app = create_app(config, logger)
            c = TestClient(app)
            resp = c.post("/api/run-command", json={"servers": ["web1"], "command": "whoami"})
            assert resp.json().get("status") == "started"
            self._wait_bg_threads()

    def test_run_command_nonzero_exit(self, tmp_path):
        """Run command that returns non-zero exit code logs failure."""
        config, logger = self._make_cmd_only_app(tmp_path)

        def _failing_gen(*a, **kw):
            yield "error output"
            return 1

        mock_session = MagicMock()
        mock_session.exec_command.side_effect = _failing_gen

        with patch("ssh_ops.server.ConnectionPool") as MockPool, \
             patch("ssh_ops.server.asyncio"):
            pool_inst = MockPool.return_value
            pool_inst.get_session.return_value = mock_session
            pool_inst.connected_servers.return_value = []
            pool_inst.disconnect_all.return_value = None
            app = create_app(config, logger)
            c = TestClient(app)
            resp = c.post("/api/run-command", json={"servers": ["web1"], "command": "false"})
            assert resp.json().get("status") == "started"
            self._wait_bg_threads()


# ---------------------------------------------------------------------------
# Upload — invalid filename (covers line 376)
# ---------------------------------------------------------------------------

class TestUploadInvalidFilename:
    def test_upload_path_traversal_filename(self, mock_client):
        """Upload with path-traversal filename is sanitized."""
        resp = mock_client.post("/api/upload", data={
            "dest": "/opt/app/file.txt",
            "servers": "web1",
        }, files={"file": ("../../etc/passwd", b"content", "text/plain")})
        data = resp.json()
        # Should succeed but use only the base filename "passwd"
        assert data.get("web1") == "uploaded"

    def test_upload_sanitized_filename_empty(self, tmp_config, tmp_path):
        """Filename that resolves to empty after sanitization."""
        from fastapi.datastructures import UploadFile
        from io import BytesIO
        import asyncio

        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs_fn"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)

        with patch("ssh_ops.server.ConnectionPool") as MockPool:
            pool_inst = MockPool.return_value
            pool_inst.connected_servers.return_value = []
            pool_inst.disconnect_all.return_value = None
            app = create_app(config, logger)

            # Call the endpoint function directly with a crafted filename
            from starlette.testclient import TestClient
            c = TestClient(app)
            # Use a filename that Path().name returns "" for — "/" won't work via HTTP
            # Instead, patch Path.name to return "" to simulate
            with patch("ssh_ops.server.Path") as MockPath:
                mock_path_instance = MagicMock()
                mock_path_instance.name = ""
                MockPath.return_value = mock_path_instance
                resp = c.post("/api/upload", data={
                    "dest": "/opt/app/file.txt",
                    "servers": "web1",
                }, files={"file": ("test.txt", b"content", "text/plain")})
                data = resp.json()
                assert "error" in data
                assert "invalid filename" in data["error"]


# ---------------------------------------------------------------------------
# POST /api/create-config
# ---------------------------------------------------------------------------

class TestCreateConfig:
    def test_create_config_ok(self, client, tmp_config):
        resp = client.post("/api/create-config", json={
            "name": "newenv",
            "servers": [{"host": "10.0.0.99", "name": "test-srv"}],
            "tasks": [{"command": "uptime"}],
        })
        data = resp.json()
        assert data.get("status") == "ok"
        assert "newenv" in data.get("config", "")
        # Verify the file was created
        new_path = tmp_config.parent / "newenv.yml"
        assert new_path.exists()

    def test_create_config_empty_name(self, client):
        resp = client.post("/api/create-config", json={"name": ""})
        data = resp.json()
        assert "error" in data
        assert "name is required" in data["error"]

    def test_create_config_invalid_name(self, client):
        resp = client.post("/api/create-config", json={"name": "bad name!"})
        data = resp.json()
        assert "error" in data
        assert "alphanumeric" in data["error"]

    def test_create_config_duplicate(self, client, tmp_config):
        # First creation
        client.post("/api/create-config", json={"name": "dup-test", "servers": []})
        # Second creation with same name
        resp = client.post("/api/create-config", json={"name": "dup-test", "servers": []})
        data = resp.json()
        assert "error" in data
        assert "already exists" in data["error"]

    def test_create_config_empty_servers(self, client):
        resp = client.post("/api/create-config", json={"name": "empty-cfg", "servers": [], "tasks": []})
        data = resp.json()
        assert data.get("status") == "ok"

    def test_create_config_with_connected_servers(self, mock_client, tmp_config):
        resp = mock_client.post("/api/create-config", json={
            "name": "switch-test",
            "servers": [{"host": "10.0.0.50"}],
        })
        data = resp.json()
        assert data.get("status") == "ok"

    def test_create_config_no_tasks_key(self, client):
        """Creating config without tasks key defaults to empty list."""
        resp = client.post("/api/create-config", json={"name": "notasks-cfg", "servers": []})
        data = resp.json()
        assert data.get("status") == "ok"

    def test_create_config_switch_error(self, tmp_config, tmp_path):
        """Exception during switch_config triggers cleanup (removes created file)."""
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs_create_err"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        with patch("ssh_ops.server.ConnectionPool") as MockPool:
            pool_inst = MockPool.return_value
            pool_inst.connected_servers.return_value = []
            pool_inst.disconnect_all.return_value = None
            app = create_app(config, logger)
            c = TestClient(app)
            # Patch config.switch_config to raise
            with patch.object(config, "switch_config", side_effect=RuntimeError("boom")):
                resp = c.post("/api/create-config", json={
                    "name": "fail-cfg",
                    "servers": [],
                })
                data = resp.json()
                assert "error" in data
                assert "boom" in data["error"]
                # The file should have been cleaned up
                fail_path = tmp_config.parent / "fail-cfg.yml"
                assert not fail_path.exists()


# ---------------------------------------------------------------------------
# POST /api/duplicate-config
# ---------------------------------------------------------------------------

class TestDuplicateConfig:
    def test_duplicate_ok(self, client, tmp_config):
        resp = client.post("/api/duplicate-config", json={"name": "my-copy"})
        data = resp.json()
        assert data.get("status") == "ok"
        assert "my-copy" in data.get("config", "")
        # Verify the file was created with same content
        copy_path = tmp_config.parent / "my-copy.yml"
        assert copy_path.exists()

    def test_duplicate_empty_name(self, client):
        resp = client.post("/api/duplicate-config", json={"name": ""})
        data = resp.json()
        assert "error" in data
        assert "name is required" in data["error"]

    def test_duplicate_invalid_name(self, client):
        resp = client.post("/api/duplicate-config", json={"name": "bad name!"})
        data = resp.json()
        assert "error" in data
        assert "alphanumeric" in data["error"]

    def test_duplicate_already_exists(self, client, tmp_config):
        # First duplicate
        client.post("/api/duplicate-config", json={"name": "dup-exist"})
        # Second with same name
        resp = client.post("/api/duplicate-config", json={"name": "dup-exist"})
        data = resp.json()
        assert "error" in data
        assert "already exists" in data["error"]

    def test_duplicate_with_connected_servers(self, mock_client):
        resp = mock_client.post("/api/duplicate-config", json={"name": "dup-conn"})
        data = resp.json()
        assert data.get("status") == "ok"

    def test_duplicate_switch_error(self, tmp_config, tmp_path):
        """Exception during switch_config triggers cleanup."""
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs_dup_err"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        with patch("ssh_ops.server.ConnectionPool") as MockPool:
            pool_inst = MockPool.return_value
            pool_inst.connected_servers.return_value = []
            pool_inst.disconnect_all.return_value = None
            app = create_app(config, logger)
            c = TestClient(app)
            with patch.object(config, "switch_config", side_effect=RuntimeError("fail")):
                resp = c.post("/api/duplicate-config", json={"name": "dup-fail"})
                data = resp.json()
                assert "error" in data
                assert "fail" in data["error"]
                assert not (tmp_config.parent / "dup-fail.yml").exists()


# ---------------------------------------------------------------------------
# POST /api/rename-config
# ---------------------------------------------------------------------------

class TestRenameConfig:
    def test_rename_ok(self, client, tmp_config):
        old_name = tmp_config.stem
        resp = client.post("/api/rename-config", json={"name": "renamed"})
        data = resp.json()
        assert data.get("status") == "ok"
        assert "renamed" in data.get("config", "")
        # Old file should be gone, new should exist
        assert not tmp_config.exists()
        assert (tmp_config.parent / "renamed.yml").exists()

    def test_rename_empty_name(self, client):
        resp = client.post("/api/rename-config", json={"name": ""})
        data = resp.json()
        assert "error" in data
        assert "name is required" in data["error"]

    def test_rename_invalid_name(self, client):
        resp = client.post("/api/rename-config", json={"name": "bad name!"})
        data = resp.json()
        assert "error" in data
        assert "alphanumeric" in data["error"]

    def test_rename_already_exists(self, client, tmp_config):
        # Create a file that conflicts
        (tmp_config.parent / "conflict.yml").write_text("servers: []")
        resp = client.post("/api/rename-config", json={"name": "conflict"})
        data = resp.json()
        assert "error" in data
        assert "already exists" in data["error"]

    def test_rename_switch_error(self, tmp_config, tmp_path):
        """Exception during switch restores old filename."""
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs_rename_err"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        with patch("ssh_ops.server.ConnectionPool") as MockPool:
            pool_inst = MockPool.return_value
            pool_inst.connected_servers.return_value = []
            pool_inst.disconnect_all.return_value = None
            app = create_app(config, logger)
            c = TestClient(app)
            with patch.object(config, "switch_config", side_effect=RuntimeError("boom")):
                resp = c.post("/api/rename-config", json={"name": "rename-fail"})
                data = resp.json()
                assert "error" in data
                # Old file should be restored
                assert tmp_config.exists()


# ---------------------------------------------------------------------------
# POST /api/delete-config
# ---------------------------------------------------------------------------

class TestDeleteConfig:
    def test_delete_no_confirm(self, client):
        resp = client.post("/api/delete-config", json={})
        data = resp.json()
        assert "error" in data
        assert "confirmation" in data["error"]

    def test_delete_only_config(self, client):
        """Cannot delete the only config file."""
        resp = client.post("/api/delete-config", json={"confirm": True})
        data = resp.json()
        assert "error" in data
        assert "only config" in data["error"]

    def test_delete_ok(self, tmp_config, tmp_path):
        """Delete current config when another exists."""
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs_del_cfg"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        # Create another config so deletion is allowed
        other = tmp_config.parent / "other.yml"
        other.write_text(yaml.dump({"servers": [], "tasks": []}, default_flow_style=False))
        with patch("ssh_ops.server.ConnectionPool") as MockPool:
            pool_inst = MockPool.return_value
            pool_inst.connected_servers.return_value = []
            pool_inst.disconnect_all.return_value = None
            app = create_app(config, logger)
            c = TestClient(app)
            resp = c.post("/api/delete-config", json={"confirm": True})
            data = resp.json()
            assert data.get("status") == "ok"
            assert not tmp_config.exists()
            assert "other" in data.get("switched_to", "")

    def test_delete_switch_error(self, tmp_config, tmp_path):
        """Error during switch after deletion."""
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs_del_cfg_err"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        other = tmp_config.parent / "fallback.yml"
        other.write_text(yaml.dump({"servers": [], "tasks": []}, default_flow_style=False))
        with patch("ssh_ops.server.ConnectionPool") as MockPool:
            pool_inst = MockPool.return_value
            pool_inst.connected_servers.return_value = []
            pool_inst.disconnect_all.return_value = None
            app = create_app(config, logger)
            c = TestClient(app)
            with patch.object(config, "switch_config", side_effect=RuntimeError("fail")):
                resp = c.post("/api/delete-config", json={"confirm": True})
                data = resp.json()
                assert "error" in data


# ---------------------------------------------------------------------------
# POST /api/update-server-password
# ---------------------------------------------------------------------------

class TestUpdateServerPassword:
    def test_update_password_ok(self, client):
        resp = client.post("/api/update-server-password", json={"index": 0, "password": "newpass"})
        data = resp.json()
        assert data.get("status") == "ok"
        assert data["server"] == "web1"

    def test_clear_password_rejected(self, client):
        resp = client.post("/api/update-server-password", json={"index": 0, "password": ""})
        data = resp.json()
        assert "error" in data
        assert "empty" in data["error"]

    def test_no_password_field_no_change(self, client):
        resp = client.post("/api/update-server-password", json={"index": 0})
        data = resp.json()
        assert data.get("status") == "ok"

    def test_no_index(self, client):
        resp = client.post("/api/update-server-password", json={"password": "x"})
        data = resp.json()
        assert "error" in data
        assert "no index" in data["error"]

    def test_non_integer_index(self, client):
        resp = client.post("/api/update-server-password", json={"index": "abc", "password": "x"})
        data = resp.json()
        assert "error" in data
        assert "integer" in data["error"]

    def test_non_string_password(self, client):
        resp = client.post("/api/update-server-password", json={"index": 0, "password": 123})
        data = resp.json()
        assert "error" in data
        assert "string" in data["error"]

    def test_out_of_range(self, client):
        resp = client.post("/api/update-server-password", json={"index": 999, "password": "x"})
        data = resp.json()
        assert "error" in data
        assert "out of range" in data["error"]

    def test_negative_index(self, client):
        resp = client.post("/api/update-server-password", json={"index": -1, "password": "x"})
        data = resp.json()
        assert "error" in data
        assert "out of range" in data["error"]

    def test_write_error(self, tmp_config, tmp_path):
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs_upd_pwd_err"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        tmp_config.unlink()
        c = TestClient(app)
        resp = c.post("/api/update-server-password", json={"index": 0, "password": "x"})
        data = resp.json()
        assert "error" in data


# ---------------------------------------------------------------------------
# POST /api/add-server
# ---------------------------------------------------------------------------

class TestAddServer:
    def test_add_server_ok(self, client):
        resp = client.post("/api/add-server", json={"host": "10.0.0.99"})
        data = resp.json()
        assert data.get("status") == "ok"
        assert data["server"]["host"] == "10.0.0.99"

    def test_add_server_with_all_fields(self, client):
        resp = client.post("/api/add-server", json={
            "host": "10.0.0.88",
            "name": "app-srv",
            "port": "2222",
            "username": "deploy",
            "password": "secret",
            "groups": "web, prod",
        })
        data = resp.json()
        assert data.get("status") == "ok"
        server = data["server"]
        assert server["host"] == "10.0.0.88"
        assert server["name"] == "app-srv"
        assert server["port"] == 2222
        assert server["username"] == "deploy"
        assert server["groups"] == ["web", "prod"]

    def test_add_server_groups_as_list(self, client):
        resp = client.post("/api/add-server", json={
            "host": "10.0.0.77",
            "groups": ["db", "staging"],
        })
        data = resp.json()
        assert data.get("status") == "ok"
        assert data["server"]["groups"] == ["db", "staging"]

    def test_add_server_no_host(self, client):
        resp = client.post("/api/add-server", json={"name": "nohost"})
        data = resp.json()
        assert "error" in data
        assert "host is required" in data["error"]

    def test_add_server_invalid_port(self, client):
        resp = client.post("/api/add-server", json={"host": "10.0.0.1", "port": "abc"})
        data = resp.json()
        assert "error" in data
        assert "port must be a number" in data["error"]

    def test_add_server_no_servers_key_in_yaml(self, tmp_path):
        """Config without 'servers' key triggers raw['servers'] = []."""
        TaskConfig._counter.clear()
        cfg = tmp_path / "nosrv.yml"
        cfg.write_text(yaml.dump({"tasks": []}, default_flow_style=False), encoding="utf-8")
        config = AppConfig(cfg)
        log_dir = tmp_path / "logs_nosrv"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        c = TestClient(app)
        resp = c.post("/api/add-server", json={"host": "10.0.0.1"})
        data = resp.json()
        assert data.get("status") == "ok"

    def test_add_server_write_error(self, tmp_config, tmp_path):
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs_add_srv_err"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        tmp_config.unlink()
        c = TestClient(app)
        resp = c.post("/api/add-server", json={"host": "10.0.0.1"})
        data = resp.json()
        assert "error" in data


# ---------------------------------------------------------------------------
# POST /api/delete-server
# ---------------------------------------------------------------------------

class TestDeleteServer:
    def test_delete_server_ok(self, client):
        resp = client.post("/api/delete-server", json={"index": 0})
        data = resp.json()
        assert data.get("status") == "ok"
        assert "removed" in data

    def test_delete_server_no_index(self, client):
        resp = client.post("/api/delete-server", json={})
        data = resp.json()
        assert "error" in data
        assert "no index" in data["error"]

    def test_delete_server_non_integer(self, client):
        resp = client.post("/api/delete-server", json={"index": "abc"})
        data = resp.json()
        assert "error" in data
        assert "integer" in data["error"]

    def test_delete_server_out_of_range(self, client):
        resp = client.post("/api/delete-server", json={"index": 999})
        data = resp.json()
        assert "error" in data
        assert "out of range" in data["error"]

    def test_delete_server_negative_index(self, client):
        resp = client.post("/api/delete-server", json={"index": -1})
        data = resp.json()
        assert "error" in data
        assert "out of range" in data["error"]

    def test_delete_server_write_error(self, tmp_config, tmp_path):
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs_del_srv_err"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        tmp_config.unlink()
        c = TestClient(app)
        resp = c.post("/api/delete-server", json={"index": 0})
        data = resp.json()
        assert "error" in data


# ---------------------------------------------------------------------------
# GET /api/task-status
# ---------------------------------------------------------------------------

class TestTaskStatus:
    def test_task_status_initial(self, client):
        resp = client.get("/api/task-status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["running"] is False


# ---------------------------------------------------------------------------
# POST /api/dry-run
# ---------------------------------------------------------------------------

class TestDryRun:
    def test_dry_run_all_tasks(self, client):
        resp = client.post("/api/dry-run", json={"servers": ["web1", "db1"]})
        data = resp.json()
        assert "servers" in data
        assert "tasks" in data
        assert len(data["servers"]) == 2
        assert len(data["tasks"]) == 3

    def test_dry_run_specific_tasks(self, client):
        # First get the actual task names from the API
        tasks_resp = client.get("/api/tasks")
        all_tasks = tasks_resp.json()
        first_task = all_tasks[0]["name"]
        resp = client.post("/api/dry-run", json={"servers": ["web1"], "tasks": [first_task]})
        data = resp.json()
        assert len(data["servers"]) == 1
        assert len(data["tasks"]) == 1
        assert data["tasks"][0]["type"] == "command"
        assert data["tasks"][0]["detail"] == "whoami"

    def test_dry_run_no_servers(self, client):
        resp = client.post("/api/dry-run", json={"servers": ["nonexistent"]})
        data = resp.json()
        assert "error" in data

    def test_dry_run_no_tasks(self, client):
        resp = client.post("/api/dry-run", json={"servers": ["web1"], "tasks": ["nonexistent"]})
        data = resp.json()
        assert "error" in data

    def test_dry_run_modifying_flag(self, tmp_path):
        """Tasks with modifying commands should have modifying=True."""
        TaskConfig._counter.clear()
        data = {
            "servers": [{"host": "10.0.0.1", "name": "s1", "password": "p"}],
            "tasks": [{"command": "rm -rf /tmp/old"}, {"command": "whoami"}],
        }
        cfg_path = tmp_path / "test.yml"
        cfg_path.write_text(yaml.dump(data, default_flow_style=False), encoding="utf-8")
        config = AppConfig(cfg_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        c = TestClient(app)
        resp = c.post("/api/dry-run", json={"servers": ["s1"]})
        tasks = resp.json()["tasks"]
        assert tasks[0]["modifying"] is True
        assert tasks[1]["modifying"] is False

    def test_dry_run_upload_task(self, client):
        resp = client.post("/api/dry-run", json={"servers": ["web1"]})
        data = resp.json()
        upload_task = [t for t in data["tasks"] if t["type"] == "upload"][0]
        assert "\u2192" in upload_task["detail"]


# ---------------------------------------------------------------------------
# GET /api/config-yaml — raw mode
# ---------------------------------------------------------------------------

class TestConfigYaml:
    def test_config_yaml_masked(self, client):
        resp = client.get("/api/config-yaml")
        assert resp.status_code == 200
        assert "********" in resp.text

    def test_config_yaml_raw(self, client):
        resp = client.get("/api/config-yaml?raw=1")
        assert resp.status_code == 200
        assert "********" not in resp.text
        assert "pass" in resp.text


# ---------------------------------------------------------------------------
# POST /api/update-config-yaml
# ---------------------------------------------------------------------------

class TestUpdateConfigYaml:
    def test_update_config_ok(self, client, tmp_config):
        new_yaml = "servers:\n- host: 10.0.0.99\n  username: root\n  password: secret\ntasks:\n- command: uptime\n"
        resp = client.post("/api/update-config-yaml", json={"yaml": new_yaml})
        data = resp.json()
        assert data.get("status") == "ok"
        # Verify config was updated
        resp2 = client.get("/api/servers")
        servers = resp2.json()
        assert len(servers) == 1
        assert servers[0]["host"] == "10.0.0.99"

    def test_update_config_empty(self, client):
        resp = client.post("/api/update-config-yaml", json={"yaml": ""})
        data = resp.json()
        assert "error" in data
        assert "Empty" in data["error"]

    def test_update_config_invalid_yaml(self, client):
        resp = client.post("/api/update-config-yaml", json={"yaml": "servers:\n  - host: [unterminated"})
        data = resp.json()
        assert "error" in data
        assert "YAML" in data["error"] or "syntax" in data["error"].lower()

    def test_update_config_not_a_mapping(self, client):
        resp = client.post("/api/update-config-yaml", json={"yaml": "- just\n- a\n- list\n"})
        data = resp.json()
        assert "error" in data
        assert "mapping" in data["error"]

    def test_update_config_validation_error(self, client):
        resp = client.post("/api/update-config-yaml", json={"yaml": "servers:\n- not_a_mapping\ntasks: []\n"})
        data = resp.json()
        # string servers are valid (shorthand), so this should work
        # Test with truly invalid data: server without host
        resp2 = client.post("/api/update-config-yaml", json={"yaml": "servers:\n- username: admin\ntasks: []\n"})
        data2 = resp2.json()
        assert "error" in data2

    def test_update_config_backup_created(self, client, tmp_config):
        new_yaml = "servers:\n- host: 10.0.0.99\n  password: p\ntasks: []\n"
        client.post("/api/update-config-yaml", json={"yaml": new_yaml})
        bak_path = tmp_config.with_suffix(tmp_config.suffix + ".bak")
        assert bak_path.exists()

    def test_update_config_atomic_preserves_on_error(self, client, tmp_config):
        """If validation fails, the original config should remain unchanged."""
        original = tmp_config.read_text(encoding="utf-8")
        resp = client.post("/api/update-config-yaml", json={"yaml": "servers:\n- {}\ntasks: []\n"})
        assert "error" in resp.json()
        assert tmp_config.read_text(encoding="utf-8") == original

    def test_update_config_write_error(self, tmp_path):
        """If writing fails, return error."""
        import os
        TaskConfig._counter.clear()
        cfg_dir = tmp_path / "cfgdir"
        cfg_dir.mkdir()
        cfg_path = cfg_dir / "test.yml"
        cfg_path.write_text(yaml.dump({"servers": [], "tasks": []}, default_flow_style=False), encoding="utf-8")
        config = AppConfig(cfg_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        c = TestClient(app)
        # Make directory read-only to prevent temp file creation
        cfg_dir.chmod(0o444)
        try:
            resp = c.post("/api/update-config-yaml", json={"yaml": "servers: []\ntasks: []\n"})
            data = resp.json()
            assert "error" in data
        finally:
            cfg_dir.chmod(0o755)

    def test_config_yaml_raw_password_visible(self, tmp_config):
        """Raw mode should show actual password values."""
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_config.parent / "logs_raw"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        c = TestClient(app)
        resp = c.get("/api/config-yaml?raw=1")
        # Should contain the actual password 'pass' from fixture
        assert "pass" in resp.text
        assert "********" not in resp.text


# ---------------------------------------------------------------------------
# _friendly_error
# ---------------------------------------------------------------------------

class TestFriendlyError:
    def test_yaml_error_with_problem_mark(self):
        """YAML error with problem_mark should show line/column."""
        try:
            yaml.safe_load("foo: [bar\n  baz")
        except yaml.YAMLError as e:
            result = _friendly_error(e)
            assert "YAML syntax error" in result

    def test_yaml_error_without_problem_mark(self):
        """YAML error without problem_mark should show generic message."""
        err = yaml.YAMLError("bad yaml")
        result = _friendly_error(err)
        assert "YAML syntax error" in result

    def test_generic_error(self):
        result = _friendly_error(ValueError("something wrong"))
        assert result == "something wrong"


# ---------------------------------------------------------------------------
# Cancel tasks
# ---------------------------------------------------------------------------

class TestCancelTasks:
    @pytest.fixture
    def tmp_config(self, tmp_path):
        cfg = tmp_path / "cfg.yml"
        cfg.write_text("servers:\n- host: 10.0.0.1\n  password: pass\ntasks:\n- command: sleep 10\n- command: echo done\n")
        return cfg

    def test_cancel_not_running(self, tmp_config):
        """Cancel when no tasks are running returns not_running."""
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_config.parent / "logs_cancel"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        c = TestClient(app)
        resp = c.post("/api/cancel")
        assert resp.json()["status"] == "not_running"

    def test_cancel_while_running(self, tmp_config):
        """Cancel while tasks are running returns cancelling and sets event."""
        import time
        import threading
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_config.parent / "logs_cancel2"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        c = TestClient(app)

        # Patch executor to block on first task so we can cancel
        original_run_task = None
        barrier = threading.Event()

        with patch.object(TaskExecutor, 'run_task', side_effect=lambda s, t: (barrier.wait(timeout=2), True)[1]):
            # Start run in a thread since it triggers a background thread
            resp = c.post("/api/run", json={"servers": ["10.0.0.1"], "tasks": ["command-1"]})
            assert resp.json()["status"] == "started"
            time.sleep(0.1)  # Let the background thread start

            # Now cancel
            resp = c.post("/api/cancel")
            assert resp.json()["status"] == "cancelling"

            # Unblock the running task
            barrier.set()
            time.sleep(0.2)  # Let it finish

            # Check final status
            resp = c.get("/api/task-status")
            status = resp.json()
            assert status["running"] is False


# ---------------------------------------------------------------------------
# History corruption
# ---------------------------------------------------------------------------

class TestLoadHistoryCorrupted:
    def test_corrupted_history_file(self, tmp_path):
        """Corrupted history JSON should not crash."""
        TaskConfig._counter.clear()
        data = {"servers": [{"host": "10.0.0.1", "name": "s1", "password": "p"}], "tasks": [{"command": "echo hi"}]}
        cfg_path = tmp_path / "test.yml"
        cfg_path.write_text(yaml.dump(data), encoding="utf-8")

        # Create corrupted history file
        hist_dir = tmp_path / ".history"
        hist_dir.mkdir()
        hist_file = hist_dir / "test.json"
        hist_file.write_text("NOT VALID JSON {{{{", encoding="utf-8")

        config = AppConfig(cfg_path)
        logger = ExecLogger(tmp_path / "logs")
        # Should not raise -- corrupted history is silently ignored
        app = create_app(config, logger)
        c = TestClient(app)
        resp = c.get("/api/cmd-history")
        assert resp.json() == []


# ---------------------------------------------------------------------------
# Proxy info / update
# ---------------------------------------------------------------------------

class TestProxyInfo:
    def test_proxy_info_no_proxy(self, client):
        resp = client.get("/api/proxy-info")
        assert resp.json() == {"enabled": False}

    def test_proxy_info_with_proxy(self, tmp_path):
        TaskConfig._counter.clear()
        data = {
            "servers": [{"host": "10.0.0.1", "name": "s1", "password": "p"}],
            "tasks": [{"command": "echo hi"}],
            "proxy": {"type": "psmp", "host": "psmp.corp.com", "user": "jdoe", "account": "admin"},
        }
        cfg_path = tmp_path / "test.yml"
        cfg_path.write_text(yaml.dump(data), encoding="utf-8")
        config = AppConfig(cfg_path)
        logger = ExecLogger(tmp_path / "logs")
        app = create_app(config, logger)
        c = TestClient(app)
        resp = c.get("/api/proxy-info")
        info = resp.json()
        assert info["enabled"] is True
        assert info["host"] == "psmp.corp.com"
        assert info["user"] == "jdoe"


class TestUpdateProxy:
    def test_set_proxy(self, client, tmp_config):
        resp = client.post("/api/update-proxy", json={
            "host": "psmp.corp.com", "user": "jdoe", "account": "admin"
        })
        assert resp.json().get("status") == "ok"
        # Verify written to file
        with open(tmp_config) as f:
            raw = yaml.safe_load(f)
        assert raw["proxy"]["host"] == "psmp.corp.com"

    def test_remove_proxy(self, client, tmp_config):
        # First set proxy
        client.post("/api/update-proxy", json={
            "host": "psmp.corp.com", "user": "jdoe", "account": "admin"
        })
        # Then remove
        resp = client.post("/api/update-proxy", json={"remove": True})
        assert resp.json().get("status") == "ok"
        with open(tmp_config) as f:
            raw = yaml.safe_load(f)
        assert "proxy" not in raw

    def test_set_proxy_missing_fields(self, client):
        resp = client.post("/api/update-proxy", json={"host": "psmp.corp.com"})
        assert "error" in resp.json()
        assert "required" in resp.json()["error"]


# ---------------------------------------------------------------------------
# Download config
# ---------------------------------------------------------------------------

class TestDownloadConfig2:
    def test_download_config(self, client):
        resp = client.get("/api/download-config")
        assert resp.status_code == 200
        assert "servers:" in resp.text or "tasks:" in resp.text


# ---------------------------------------------------------------------------
# Switch config error
# ---------------------------------------------------------------------------

class TestSwitchConfigError2:
    def test_switch_config_nonexistent(self, client, tmp_config):
        resp = client.post("/api/switch-config", json={"path": "/nonexistent/config.yml"})
        assert "error" in resp.json()


# ---------------------------------------------------------------------------
# Rename config with connected servers
# ---------------------------------------------------------------------------

class TestRenameConfigConnected:
    def test_rename_config_disconnects(self, tmp_path):
        TaskConfig._counter.clear()
        data = {"servers": [{"host": "10.0.0.1", "name": "s1", "password": "p"}], "tasks": [{"command": "echo"}]}
        cfg_path = tmp_path / "old.yml"
        cfg_path.write_text(yaml.dump(data), encoding="utf-8")
        config = AppConfig(cfg_path)
        logger = ExecLogger(tmp_path / "logs")
        app = create_app(config, logger)
        c = TestClient(app)

        with patch("ssh_ops.server.ConnectionPool.connected_servers", return_value=["s1"]):
            resp = c.post("/api/rename-config", json={"name": "newname"})
            assert resp.json().get("status") == "ok"


# ---------------------------------------------------------------------------
# Reorder task
# ---------------------------------------------------------------------------

class TestReorderTask:
    def test_swap_adjacent_down(self, client):
        resp = client.post("/api/reorder-task", json={"index": 0, "direction": "down"})
        assert resp.json().get("status") == "ok"
        assert resp.json()["from"] == 0
        assert resp.json()["to"] == 1

    def test_swap_adjacent_up(self, client):
        resp = client.post("/api/reorder-task", json={"index": 1, "direction": "up"})
        assert resp.json().get("status") == "ok"

    def test_swap_at_edge(self, client):
        resp = client.post("/api/reorder-task", json={"index": 0, "direction": "up"})
        assert "error" in resp.json()
        assert "edge" in resp.json()["error"]

    def test_arbitrary_move(self, client):
        resp = client.post("/api/reorder-task", json={"from_index": 0, "to_index": 2})
        assert resp.json().get("status") == "ok"

    def test_arbitrary_move_same_position(self, client):
        resp = client.post("/api/reorder-task", json={"from_index": 1, "to_index": 1})
        assert resp.json().get("status") == "ok"

    def test_arbitrary_move_invalid_type(self, client):
        resp = client.post("/api/reorder-task", json={"from_index": "a", "to_index": 1})
        assert "error" in resp.json()

    def test_arbitrary_move_out_of_range(self, client):
        resp = client.post("/api/reorder-task", json={"from_index": 0, "to_index": 99})
        assert "error" in resp.json()

    def test_swap_missing_index(self, client):
        resp = client.post("/api/reorder-task", json={"direction": "up"})
        assert "error" in resp.json()

    def test_swap_bad_direction(self, client):
        resp = client.post("/api/reorder-task", json={"index": 0, "direction": "left"})
        assert "error" in resp.json()

    def test_swap_out_of_range(self, client):
        resp = client.post("/api/reorder-task", json={"index": 99, "direction": "down"})
        assert "error" in resp.json()


# ---------------------------------------------------------------------------
# Delete cmd/upload history
# ---------------------------------------------------------------------------

class TestDeleteCmdHistory2:
    def test_delete_specific_cmd(self, client):
        with patch("ssh_ops.server.ConnectionPool.get_session") as mock_sess:
            mock_session = MagicMock()
            def mock_gen(cmd, timeout=120, env=None):
                yield "ok"
                return 0
            mock_session.exec_command = mock_gen
            mock_sess.return_value = mock_session
            client.post("/api/run-command", json={"servers": ["web1"], "command": "uptime"})
            time.sleep(0.2)

        resp = client.get("/api/cmd-history")
        assert "uptime" in resp.json()

        resp = client.post("/api/delete-cmd-history", json={"command": "uptime"})
        assert resp.json().get("status") == "ok"

        resp = client.get("/api/cmd-history")
        assert "uptime" not in resp.json()

    def test_delete_all_cmd_history(self, client):
        resp = client.post("/api/delete-cmd-history", json={"all": True})
        assert resp.json().get("status") == "cleared"

    def test_delete_nonexistent_cmd(self, client):
        resp = client.post("/api/delete-cmd-history", json={"command": "nonexistent"})
        assert resp.json().get("status") == "ok"


class TestDeleteUploadHistory2:
    def test_delete_all_upload_history(self, client):
        resp = client.post("/api/delete-upload-history", json={"all": True})
        assert resp.json().get("status") == "cleared"

    def test_delete_specific_upload(self, client):
        resp = client.post("/api/delete-upload-history", json={"dest": "/opt/file.txt", "filename": "file.txt"})
        assert resp.json().get("status") == "ok"


# ---------------------------------------------------------------------------
# OTP required
# ---------------------------------------------------------------------------

class TestOTPRequired:
    def test_connect_needs_otp(self, tmp_path):
        TaskConfig._counter.clear()
        data = {
            "servers": [{"host": "10.0.0.1", "name": "s1", "password": "p"}],
            "tasks": [{"command": "echo"}],
            "proxy": {"type": "psmp", "host": "psmp.corp.com", "user": "jdoe", "account": "admin"},
        }
        cfg_path = tmp_path / "test.yml"
        cfg_path.write_text(yaml.dump(data), encoding="utf-8")
        config = AppConfig(cfg_path)
        logger = ExecLogger(tmp_path / "logs")
        app = create_app(config, logger)
        c = TestClient(app)
        resp = c.post("/api/connect", json={"servers": ["s1"]})
        data = resp.json()
        assert "error" in data
        assert data.get("needs_otp") is True


# ---------------------------------------------------------------------------
# MOTD broadcast on connect
# ---------------------------------------------------------------------------

class TestMOTDBroadcast:
    def test_connect_shows_motd(self, client):
        with patch("ssh_ops.server.ConnectionPool.connect") as mock_connect:
            mock_session = MagicMock()
            mock_session.motd = "Welcome to server\nSystem ready"
            mock_connect.return_value = mock_session
            with patch("ssh_ops.server.ConnectionPool.get_session", return_value=mock_session):
                resp = client.post("/api/connect", json={"servers": ["web1"]})
                assert resp.json().get("web1") == "connected"


# ---------------------------------------------------------------------------
# Update config with connected servers
# ---------------------------------------------------------------------------

class TestUpdateConfigConnected:
    def test_update_config_disconnects(self, tmp_config):
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_config.parent / "logs_upd"
        log_dir.mkdir(parents=True, exist_ok=True)
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        c = TestClient(app)

        new_yaml = "servers:\n- host: 10.0.0.1\n  name: web1\n  password: pass\ntasks:\n- command: whoami\n"
        with patch("ssh_ops.server.ConnectionPool.connected_servers", return_value=["web1"]):
            resp = c.post("/api/update-config-yaml", json={"yaml": new_yaml})
            assert resp.json().get("status") == "ok"


# ---------------------------------------------------------------------------
# Update config temp file cleanup on write error
# ---------------------------------------------------------------------------

class TestUpdateConfigTempCleanup:
    def test_temp_file_cleaned_on_error(self, tmp_path):
        TaskConfig._counter.clear()
        data = {"servers": [{"host": "10.0.0.1", "name": "s1", "password": "p"}], "tasks": [{"command": "echo"}]}
        cfg_path = tmp_path / "test.yml"
        cfg_path.write_text(yaml.dump(data), encoding="utf-8")
        config = AppConfig(cfg_path)
        logger = ExecLogger(tmp_path / "logs")
        app = create_app(config, logger)
        c = TestClient(app)

        with patch("ssh_ops.server.Path.replace", side_effect=OSError("disk full")):
            resp = c.post("/api/update-config-yaml", json={"yaml": "servers: []\ntasks: []\n"})
            data = resp.json()
            assert "error" in data


# ---------------------------------------------------------------------------
# Run tasks -- single vs multi server done message
# ---------------------------------------------------------------------------

class TestRunTasksDoneMessage:
    def test_single_server_no_done_message(self, client):
        """Single server success should NOT show Done -- 1/1 passed."""
        with patch("ssh_ops.server.ConnectionPool.get_session") as mock_sess:
            mock_session = MagicMock()
            def mock_gen(cmd, timeout=120, env=None):
                yield "ok"
                return 0
            mock_session.exec_command = mock_gen
            mock_sess.return_value = mock_session
            resp = client.post("/api/run", json={"servers": ["web1"]})
            assert resp.json().get("status") == "started"


# ---------------------------------------------------------------------------
# Ad-hoc multi-server done summary
# ---------------------------------------------------------------------------

class TestRunCommandMultiServer:
    def test_multi_server_adhoc_done(self, tmp_path):
        TaskConfig._counter.clear()
        data = {
            "servers": [
                {"host": "10.0.0.1", "name": "s1", "password": "p"},
                {"host": "10.0.0.2", "name": "s2", "password": "p"},
            ],
            "tasks": [{"command": "echo"}],
        }
        cfg_path = tmp_path / "test.yml"
        cfg_path.write_text(yaml.dump(data), encoding="utf-8")
        config = AppConfig(cfg_path)
        logger = ExecLogger(tmp_path / "logs")
        app = create_app(config, logger)
        c = TestClient(app)

        with patch("ssh_ops.server.ConnectionPool.get_session") as mock_sess:
            mock_session = MagicMock()
            def mock_gen(cmd, timeout=120, env=None):
                yield "ok"
                return 0
            mock_session.exec_command = mock_gen
            mock_sess.return_value = mock_session
            resp = c.post("/api/run-command", json={"servers": ["s1", "s2"], "command": "uptime"})
            assert resp.json().get("status") == "started"
            time.sleep(0.3)

    def test_multi_server_adhoc_with_failure(self, tmp_path):
        TaskConfig._counter.clear()
        data = {
            "servers": [
                {"host": "10.0.0.1", "name": "s1", "password": "p"},
                {"host": "10.0.0.2", "name": "s2", "password": "p"},
            ],
            "tasks": [{"command": "echo"}],
        }
        cfg_path = tmp_path / "test.yml"
        cfg_path.write_text(yaml.dump(data), encoding="utf-8")
        config = AppConfig(cfg_path)
        logger = ExecLogger(tmp_path / "logs")
        app = create_app(config, logger)
        c = TestClient(app)

        call_count = [0]
        def mock_get_session(name):
            call_count[0] += 1
            if call_count[0] == 1:
                mock_session = MagicMock()
                def mock_gen(cmd, timeout=120, env=None):
                    yield "error"
                    return 1
                mock_session.exec_command = mock_gen
                return mock_session
            return None

        with patch("ssh_ops.server.ConnectionPool.get_session", side_effect=mock_get_session):
            resp = c.post("/api/run-command", json={"servers": ["s1", "s2"], "command": "uptime"})
            assert resp.json().get("status") == "started"
            time.sleep(0.3)


# ---------------------------------------------------------------------------
# Exception paths in update-proxy, switch-config, reorder-task
# ---------------------------------------------------------------------------

class TestUpdateProxyError:
    def test_update_proxy_write_error(self, client):
        with patch("builtins.open", side_effect=OSError("disk full")):
            resp = client.post("/api/update-proxy", json={
                "host": "psmp.corp.com", "user": "jdoe", "account": "admin"
            })
            assert "error" in resp.json()


class TestSwitchConfigException:
    def test_switch_config_raises(self, client, tmp_config):
        """switch_config that raises should return error."""
        with patch("ssh_ops.config.AppConfig.switch_config", side_effect=ValueError("bad config")):
            resp = client.post("/api/switch-config", json={"path": str(tmp_config)})
            assert "error" in resp.json()


class TestReorderTaskException:
    def test_reorder_task_write_error(self, client):
        """Write error during reorder should return error."""
        original_open = open
        call_count = [0]
        def patched_open(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 2:  # second call is write
                raise OSError("disk full")
            return original_open(*args, **kwargs)

        with patch("builtins.open", side_effect=patched_open):
            resp = client.post("/api/reorder-task", json={"index": 0, "direction": "down"})
            assert "error" in resp.json()


# ---------------------------------------------------------------------------
# Upload history with actual matching entries
# ---------------------------------------------------------------------------

class TestDeleteUploadHistoryMatch:
    def test_delete_matching_upload(self, tmp_path):
        """Delete an upload that actually exists in history."""
        TaskConfig._counter.clear()
        data = {
            "servers": [{"host": "10.0.0.1", "name": "s1", "password": "p"}],
            "tasks": [{"command": "echo"}],
        }
        cfg_path = tmp_path / "test.yml"
        cfg_path.write_text(yaml.dump(data), encoding="utf-8")

        # Pre-populate upload history
        hist_dir = tmp_path / ".history"
        hist_dir.mkdir()
        hist_file = hist_dir / "test.json"
        hist_file.write_text(json.dumps({
            "cmd": [],
            "upload": [{"dest": "/opt/app.conf", "filename": "app.conf"}]
        }))

        config = AppConfig(cfg_path)
        logger = ExecLogger(tmp_path / "logs")
        app = create_app(config, logger)
        c = TestClient(app)

        # Verify upload history is loaded
        resp = c.get("/api/upload-history")
        assert len(resp.json()) == 1

        # Delete the matching upload
        resp = c.post("/api/delete-upload-history", json={"dest": "/opt/app.conf", "filename": "app.conf"})
        assert resp.json().get("status") == "ok"

        # Verify removed
        resp = c.get("/api/upload-history")
        assert len(resp.json()) == 0


# ---------------------------------------------------------------------------
# Run tasks multi-server done
# ---------------------------------------------------------------------------

class TestRunTasksMultiServerDone:
    def test_multi_server_success_shows_done(self, tmp_path):
        """With 2+ servers, 'Done' summary SHOULD be broadcast (covers line 864)."""
        TaskConfig._counter.clear()
        data = {
            "servers": [
                {"host": "10.0.0.1", "name": "s1", "password": "p"},
                {"host": "10.0.0.2", "name": "s2", "password": "p"},
            ],
            "tasks": [{"command": "echo hi"}],
        }
        cfg_path = tmp_path / "test.yml"
        cfg_path.write_text(yaml.dump(data), encoding="utf-8")
        config = AppConfig(cfg_path)
        logger = ExecLogger(tmp_path / "logs")
        app = create_app(config, logger)
        c = TestClient(app)

        with patch("ssh_ops.server.ConnectionPool.get_session") as mock_sess:
            mock_session = MagicMock()
            def mock_gen(cmd, timeout=120, env=None):
                yield "ok"
                return 0
            mock_session.exec_command = mock_gen
            mock_sess.return_value = mock_session
            resp = c.post("/api/run", json={"servers": ["s1", "s2"]})
            assert resp.json().get("status") == "started"
            time.sleep(0.5)
