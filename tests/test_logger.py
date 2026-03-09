"""Tests for ssh_ops.logger — logging and WS broadcast."""

from pathlib import Path

from ssh_ops.logger import ExecLogger


class TestExecLogger:
    def test_init_creates_log_dir(self, tmp_path):
        log_dir = tmp_path / "logs"
        logger = ExecLogger(log_dir)
        assert log_dir.exists()

    def test_info_writes_to_session_log(self, tmp_path):
        log_dir = tmp_path / "logs"
        logger = ExecLogger(log_dir)
        logger.info("test message")
        session_log = log_dir / "session.log"
        assert session_log.exists()
        content = session_log.read_text()
        assert "test message" in content

    def test_error_writes_to_session_log(self, tmp_path):
        log_dir = tmp_path / "logs"
        logger = ExecLogger(log_dir)
        logger.error("something failed")
        session_log = log_dir / "session.log"
        content = session_log.read_text()
        assert "something failed" in content

    def test_debug_writes_to_session_log(self, tmp_path):
        log_dir = tmp_path / "logs"
        logger = ExecLogger(log_dir)
        logger.debug("debug info")
        session_log = log_dir / "session.log"
        content = session_log.read_text()
        assert "debug info" in content

    def test_create_exec_log(self, tmp_path):
        log_dir = tmp_path / "logs"
        logger = ExecLogger(log_dir)
        log_path = logger.create_exec_log("web1", "deploy")
        assert "web1" in str(log_path)
        assert log_path.parent.exists()

    def test_write_exec_log(self, tmp_path):
        log_dir = tmp_path / "logs"
        logger = ExecLogger(log_dir)
        log_path = logger.create_exec_log("web1", "task1")
        logger.write_exec_log(log_path, "output line 1\n")
        logger.write_exec_log(log_path, "output line 2\n")
        content = log_path.read_text()
        assert "output line 1" in content
        assert "output line 2" in content

    def test_ws_callback(self, tmp_path):
        log_dir = tmp_path / "logs"
        logger = ExecLogger(log_dir)
        received = []
        logger.register_ws_callback(lambda msg: received.append(msg))
        logger.info("broadcast test")
        assert "broadcast test" in received

    def test_ws_callback_error_prefix(self, tmp_path):
        log_dir = tmp_path / "logs"
        logger = ExecLogger(log_dir)
        received = []
        logger.register_ws_callback(lambda msg: received.append(msg))
        logger.error("bad thing")
        assert any("[ERROR]" in m for m in received)

    def test_ws_callback_replaces_previous(self, tmp_path):
        log_dir = tmp_path / "logs"
        logger = ExecLogger(log_dir)
        first = []
        second = []
        logger.register_ws_callback(lambda msg: first.append(msg))
        logger.register_ws_callback(lambda msg: second.append(msg))
        logger.info("hello")
        assert len(first) == 0  # replaced
        assert len(second) == 1

    def test_ws_callback_exception_ignored(self, tmp_path):
        log_dir = tmp_path / "logs"
        logger = ExecLogger(log_dir)

        def bad_callback(msg):
            raise RuntimeError("ws error")

        logger.register_ws_callback(bad_callback)
        # Should not raise
        logger.info("still works")

    # --- Bug 12: server_name path traversal ---

    def test_safe_name_strips_path_traversal(self, tmp_path):
        """Bug 12: server_name with path traversal is sanitized."""
        log_dir = tmp_path / "logs"
        logger = ExecLogger(log_dir)
        log_path = logger.create_exec_log("../../etc", "task1")
        # Should NOT escape the log_dir
        assert str(log_dir) in str(log_path)
        assert "/etc/" not in str(log_path)

    def test_safe_name_normal(self, tmp_path):
        log_dir = tmp_path / "logs"
        logger = ExecLogger(log_dir)
        log_path = logger.create_exec_log("web-server-1", "task1")
        assert "web-server-1" in str(log_path)

    def test_safe_name_empty(self, tmp_path):
        log_dir = tmp_path / "logs"
        logger = ExecLogger(log_dir)
        assert logger._safe_name("") == "unknown"

    def test_safe_name_slashes(self, tmp_path):
        log_dir = tmp_path / "logs"
        logger = ExecLogger(log_dir)
        result = logger._safe_name("a/b\\c")
        assert "/" not in result
        assert "\\" not in result
