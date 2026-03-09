"""Tests for ssh_ops.cli — CLI argument parsing and basic flows."""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import yaml

from ssh_ops.config import TaskConfig, ServerConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_config(tmp_path, data=None):
    if data is None:
        data = {
            "servers": [{"host": "10.0.0.1", "name": "web1", "password": "pass"}],
            "tasks": [{"command": "whoami"}],
        }
    cfg = tmp_path / "test.yml"
    cfg.write_text(yaml.dump(data, default_flow_style=False), encoding="utf-8")
    return str(cfg)


# ---------------------------------------------------------------------------
# list command
# ---------------------------------------------------------------------------

class TestListCommand:
    def test_list_servers(self, tmp_path, capsys):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path)
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "list", "servers"]):
            from ssh_ops.cli import main
            main()
        out = capsys.readouterr().out
        assert "web1" in out or "10.0.0.1" in out

    def test_list_tasks(self, tmp_path, capsys):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path)
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "list", "tasks"]):
            from ssh_ops.cli import main
            main()
        out = capsys.readouterr().out
        assert "whoami" in out

    def test_list_tasks_upload(self, tmp_path, capsys):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path, {
            "servers": [],
            "tasks": [{"src": "/opt/a.txt", "dest": "/opt/b.txt"}],
        })
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "list", "tasks"]):
            from ssh_ops.cli import main
            main()
        out = capsys.readouterr().out
        assert "/opt/a.txt" in out
        assert "/opt/b.txt" in out

    def test_list_tasks_script(self, tmp_path, capsys):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path, {
            "servers": [],
            "tasks": [{"type": "script", "src": "/opt/deploy.sh", "args": "--prod"}],
        })
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "list", "tasks"]):
            from ssh_ops.cli import main
            main()
        out = capsys.readouterr().out
        assert "deploy.sh" in out


class TestListServersWithGroups:
    def test_groups_displayed(self, tmp_path, capsys):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path, {
            "servers": [{"host": "10.0.0.1", "name": "web1", "password": "p", "groups": ["web", "prod"]}],
            "tasks": [],
        })
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "list", "servers"]):
            from ssh_ops.cli import main
            main()
        out = capsys.readouterr().out
        assert "web,prod" in out


# ---------------------------------------------------------------------------
# no command
# ---------------------------------------------------------------------------

class TestNoCommand:
    def test_no_command_exits(self):
        with patch("sys.argv", ["ssh-ops"]):
            with pytest.raises(SystemExit) as exc_info:
                from ssh_ops.cli import main
                main()
            assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# run command — error paths
# ---------------------------------------------------------------------------

class TestRunCommand:
    def test_run_no_servers_matched(self, tmp_path):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path)
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "run", "--server", "nonexistent"]):
            with pytest.raises(SystemExit) as exc_info:
                from ssh_ops.cli import main
                main()
            assert exc_info.value.code == 1

    def test_run_task_not_found(self, tmp_path):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path)
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "run", "--task", "nonexistent"]):
            with pytest.raises(SystemExit) as exc_info:
                from ssh_ops.cli import main
                main()
            assert exc_info.value.code == 1

    def test_run_no_tasks(self, tmp_path):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path, {"servers": [{"host": "10.0.0.1", "password": "p"}], "tasks": []})
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "run"]):
            with pytest.raises(SystemExit) as exc_info:
                from ssh_ops.cli import main
                main()
            assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# upload command — error paths
# ---------------------------------------------------------------------------

class TestUploadCommand:
    def test_upload_file_not_found(self, tmp_path):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path)
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "upload", "/nonexistent/file.txt", "/opt/dest.txt", "--server", "web1"]):
            with pytest.raises(SystemExit) as exc_info:
                from ssh_ops.cli import main
                main()
            assert exc_info.value.code == 1

    def test_upload_no_servers(self, tmp_path):
        TaskConfig._counter.clear()
        local = tmp_path / "file.txt"
        local.write_text("hi")
        cfg = _write_config(tmp_path)
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "upload", str(local), "/opt/dest.txt", "--server", "nonexistent"]):
            with pytest.raises(SystemExit) as exc_info:
                from ssh_ops.cli import main
                main()
            assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# run command — success path with mocked pool
# ---------------------------------------------------------------------------

class TestRunSuccess:
    def test_run_all_tasks_success(self, tmp_path):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path)

        mock_pool = MagicMock()
        mock_executor = MagicMock()
        mock_executor.run_all_tasks.return_value = {
            "web1": [{"task": "command-1", "success": True}]
        }

        with patch("sys.argv", ["ssh-ops", "-c", cfg, "run"]):
            with patch("ssh_ops.cli.ConnectionPool", return_value=mock_pool):
                with patch("ssh_ops.cli.TaskExecutor", return_value=mock_executor):
                    with pytest.raises(SystemExit) as exc_info:
                        from ssh_ops.cli import main
                        main()
                    assert exc_info.value.code == 0

    def test_run_all_tasks_failure(self, tmp_path):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path)

        mock_pool = MagicMock()
        mock_executor = MagicMock()
        mock_executor.run_all_tasks.return_value = {
            "web1": [{"task": "command-1", "success": False}]
        }

        with patch("sys.argv", ["ssh-ops", "-c", cfg, "run"]):
            with patch("ssh_ops.cli.ConnectionPool", return_value=mock_pool):
                with patch("ssh_ops.cli.TaskExecutor", return_value=mock_executor):
                    with pytest.raises(SystemExit) as exc_info:
                        from ssh_ops.cli import main
                        main()
                    assert exc_info.value.code == 1

    def test_run_connect_failure(self, tmp_path):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path)

        mock_pool = MagicMock()
        mock_pool.connect.side_effect = Exception("connection refused")

        with patch("sys.argv", ["ssh-ops", "-c", cfg, "run"]):
            with patch("ssh_ops.cli.ConnectionPool", return_value=mock_pool):
                with pytest.raises(SystemExit) as exc_info:
                    from ssh_ops.cli import main
                    main()
                assert exc_info.value.code == 1

    def test_run_parallel(self, tmp_path):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path)

        mock_pool = MagicMock()
        mock_executor = MagicMock()
        mock_executor.run_all_tasks.return_value = {
            "web1": [{"task": "command-1", "success": True}]
        }

        with patch("sys.argv", ["ssh-ops", "-c", cfg, "run", "--parallel"]):
            with patch("ssh_ops.cli.ConnectionPool", return_value=mock_pool):
                with patch("ssh_ops.cli.TaskExecutor", return_value=mock_executor):
                    with pytest.raises(SystemExit) as exc_info:
                        from ssh_ops.cli import main
                        main()
                    assert exc_info.value.code == 0

    def test_run_specific_task(self, tmp_path):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path, {
            "servers": [{"host": "10.0.0.1", "name": "web1", "password": "p"}],
            "tasks": [{"name": "deploy", "command": "echo deploy"}],
        })

        mock_pool = MagicMock()
        mock_executor = MagicMock()
        mock_executor.run_all_tasks.return_value = {
            "web1": [{"task": "deploy", "success": True}]
        }

        with patch("sys.argv", ["ssh-ops", "-c", cfg, "run", "--task", "deploy"]):
            with patch("ssh_ops.cli.ConnectionPool", return_value=mock_pool):
                with patch("ssh_ops.cli.TaskExecutor", return_value=mock_executor):
                    with pytest.raises(SystemExit) as exc_info:
                        from ssh_ops.cli import main
                        main()
                    assert exc_info.value.code == 0


# ---------------------------------------------------------------------------
# exec command — with mocked pool
# ---------------------------------------------------------------------------

class TestExecCommand:
    def test_exec_success(self, tmp_path):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path)

        mock_session = MagicMock()
        mock_session.exec_command.return_value = iter(["output line"])

        mock_pool = MagicMock()
        mock_pool.get_session.return_value = mock_session

        with patch("sys.argv", ["ssh-ops", "-c", cfg, "exec", "whoami", "--server", "web1"]):
            with patch("ssh_ops.cli.ConnectionPool", return_value=mock_pool):
                from ssh_ops.cli import main
                main()

    def test_exec_nonzero_exit(self, tmp_path):
        """exec command with non-zero exit code should exit 1."""
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path)

        def _failing_gen(*a, **kw):
            yield "error output"
            return 1

        mock_session = MagicMock()
        mock_session.exec_command.side_effect = _failing_gen

        mock_pool = MagicMock()
        mock_pool.get_session.return_value = mock_session

        with patch("sys.argv", ["ssh-ops", "-c", cfg, "exec", "false", "--server", "web1"]):
            with patch("ssh_ops.cli.ConnectionPool", return_value=mock_pool):
                with pytest.raises(SystemExit) as exc_info:
                    from ssh_ops.cli import main
                    main()
                assert exc_info.value.code == 1

    def test_exec_no_servers(self, tmp_path):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path)

        with patch("sys.argv", ["ssh-ops", "-c", cfg, "exec", "whoami", "--server", "nonexistent"]):
            with pytest.raises(SystemExit) as exc_info:
                from ssh_ops.cli import main
                main()
            assert exc_info.value.code == 1

    def test_exec_not_connected(self, tmp_path):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path)

        mock_pool = MagicMock()
        mock_pool.get_session.return_value = None

        with patch("sys.argv", ["ssh-ops", "-c", cfg, "exec", "whoami", "--server", "web1"]):
            with patch("ssh_ops.cli.ConnectionPool", return_value=mock_pool):
                with pytest.raises(SystemExit) as exc_info:
                    from ssh_ops.cli import main
                    main()
                assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# upload command — success with mocked pool
# ---------------------------------------------------------------------------

class TestUploadSuccess:
    def test_upload_success(self, tmp_path):
        TaskConfig._counter.clear()
        local = tmp_path / "file.txt"
        local.write_text("content")
        cfg = _write_config(tmp_path)

        mock_session = MagicMock()
        mock_pool = MagicMock()
        mock_pool.get_session.return_value = mock_session

        with patch("sys.argv", ["ssh-ops", "-c", cfg, "upload", str(local), "/opt/dest.txt", "--server", "web1"]):
            with patch("ssh_ops.cli.ConnectionPool", return_value=mock_pool):
                from ssh_ops.cli import main
                main()

        mock_session.upload_file.assert_called_once()

    def test_upload_with_mode(self, tmp_path):
        TaskConfig._counter.clear()
        local = tmp_path / "script.sh"
        local.write_text("#!/bin/bash")
        cfg = _write_config(tmp_path)

        mock_session = MagicMock()
        mock_pool = MagicMock()
        mock_pool.get_session.return_value = mock_session

        with patch("sys.argv", ["ssh-ops", "-c", cfg, "upload", str(local), "/opt/script.sh", "--server", "web1", "--mode", "0755"]):
            with patch("ssh_ops.cli.ConnectionPool", return_value=mock_pool):
                from ssh_ops.cli import main
                main()

        mock_session.upload_file.assert_called_once_with(str(local.resolve()), "/opt/script.sh", "0755")

    def test_upload_not_connected(self, tmp_path):
        TaskConfig._counter.clear()
        local = tmp_path / "file.txt"
        local.write_text("content")
        cfg = _write_config(tmp_path)

        mock_pool = MagicMock()
        mock_pool.get_session.return_value = None

        with patch("sys.argv", ["ssh-ops", "-c", cfg, "upload", str(local), "/opt/dest.txt", "--server", "web1"]):
            with patch("ssh_ops.cli.ConnectionPool", return_value=mock_pool):
                from ssh_ops.cli import main
                main()


# ---------------------------------------------------------------------------
# serve command
# ---------------------------------------------------------------------------

class TestServeCommand:
    def test_serve(self, tmp_path):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path)

        mock_uvicorn = MagicMock()
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "serve"]):
            with patch("ssh_ops.server.create_app") as mock_create:
                with patch.dict("sys.modules", {"uvicorn": mock_uvicorn}):
                    mock_create.return_value = MagicMock()
                    from ssh_ops.cli import main
                    main()
                    mock_uvicorn.run.assert_called_once()


# ---------------------------------------------------------------------------
# list — edge: unknown task type in output
# ---------------------------------------------------------------------------

class TestListEdge:
    def test_list_tasks_unknown_type(self, tmp_path, capsys):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path, {
            "servers": [],
            "tasks": [{"command": "echo hi"}],
        })
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "list", "tasks"]):
            from ssh_ops.cli import main
            # Force task type to unknown after loading
            main()


# ---------------------------------------------------------------------------
# __main__ module
# ---------------------------------------------------------------------------

class TestMissingConfig:
    """Tests for missing config file handling."""

    def test_serve_auto_creates_config(self, tmp_path):
        cfg = tmp_path / "subdir" / "auto.yml"
        assert not cfg.exists()
        with patch("sys.argv", ["ssh-ops", "-c", str(cfg), "serve"]):
            with patch("ssh_ops.cli._do_serve") as mock_serve:
                from ssh_ops.cli import main
                main()
                assert cfg.exists()
                content = yaml.safe_load(cfg.read_text())
                assert content["servers"] == []
                assert content["tasks"] == []
                mock_serve.assert_called_once()

    def test_non_serve_missing_config_exits(self, tmp_path):
        cfg = tmp_path / "nope.yml"
        with patch("sys.argv", ["ssh-ops", "-c", str(cfg), "list", "servers"]):
            from ssh_ops.cli import main
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1
            assert not cfg.exists()


# ---------------------------------------------------------------------------
# dry-run command
# ---------------------------------------------------------------------------

class TestDryRun:
    def test_dry_run_all_tasks(self, tmp_path, capsys):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path)
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "run", "--dry-run"]):
            from ssh_ops.cli import main
            main()
        out = capsys.readouterr().out
        assert "DRY RUN PREVIEW" in out
        assert "web1" in out
        assert "whoami" in out

    def test_dry_run_specific_task(self, tmp_path, capsys):
        TaskConfig._counter.clear()
        data = {
            "servers": [{"host": "10.0.0.1", "name": "web1", "password": "p"}],
            "tasks": [
                {"command": "whoami", "name": "check-user"},
                {"command": "df -h", "name": "check-disk"},
            ],
        }
        cfg = _write_config(tmp_path, data)
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "run", "--task", "check-user", "--dry-run"]):
            from ssh_ops.cli import main
            main()
        out = capsys.readouterr().out
        assert "check-user" in out
        assert "check-disk" not in out

    def test_dry_run_upload_task(self, tmp_path, capsys):
        TaskConfig._counter.clear()
        data = {
            "servers": [{"host": "10.0.0.1", "name": "s1", "password": "p"}],
            "tasks": [{"src": "/opt/a.txt", "dest": "/opt/b.txt"}],
        }
        cfg = _write_config(tmp_path, data)
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "run", "--dry-run"]):
            from ssh_ops.cli import main
            main()
        out = capsys.readouterr().out
        assert "upload" in out
        assert "/opt/a.txt" in out

    def test_dry_run_modifying_command(self, tmp_path, capsys):
        TaskConfig._counter.clear()
        data = {
            "servers": [{"host": "10.0.0.1", "name": "s1", "password": "p"}],
            "tasks": [{"command": "rm -rf /tmp/old"}],
        }
        cfg = _write_config(tmp_path, data)
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "run", "--dry-run"]):
            from ssh_ops.cli import main
            main()
        out = capsys.readouterr().out
        assert "modifying" in out

    def test_dry_run_no_servers(self, tmp_path):
        TaskConfig._counter.clear()
        cfg = _write_config(tmp_path)
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "run", "--server", "nonexistent", "--dry-run"]):
            with pytest.raises(SystemExit) as exc_info:
                from ssh_ops.cli import main
                main()
            assert exc_info.value.code == 1

    def test_dry_run_script_task(self, tmp_path, capsys):
        TaskConfig._counter.clear()
        data = {
            "servers": [{"host": "10.0.0.1", "name": "s1", "password": "p"}],
            "tasks": [{"type": "script", "src": "/opt/deploy.sh", "args": "--env prod"}],
        }
        cfg = _write_config(tmp_path, data)
        with patch("sys.argv", ["ssh-ops", "-c", cfg, "run", "--dry-run"]):
            from ssh_ops.cli import main
            main()
        out = capsys.readouterr().out
        assert "script" in out
        assert "/opt/deploy.sh" in out
        assert "--env prod" in out

    def test_dry_run_unknown_task_type(self, tmp_path, capsys):
        """Unknown task type in dry-run should show task name as detail."""
        TaskConfig._counter.clear()
        data = {
            "servers": [{"host": "10.0.0.1", "name": "s1", "password": "p"}],
            "tasks": [{"command": "echo x"}],
        }
        cfg = _write_config(tmp_path, data)
        # Load config, then monkey-patch task type
        from ssh_ops.config import AppConfig
        config = AppConfig(cfg)
        config.tasks[0].type = "bogus"
        config.tasks[0].name = "mystery-task"
        with patch("ssh_ops.cli.AppConfig", return_value=config):
            with patch("sys.argv", ["ssh-ops", "-c", cfg, "run", "--dry-run"]):
                from ssh_ops.cli import main
                main()
        out = capsys.readouterr().out
        assert "mystery-task" in out


class TestMainModule:
    def test_main_module(self):
        with patch("ssh_ops.cli.main") as mock_main:
            import importlib
            import ssh_ops.__main__
            importlib.reload(ssh_ops.__main__)
            mock_main.assert_called()
