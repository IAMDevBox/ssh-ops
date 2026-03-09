"""Logging — file + console + optional WebSocket broadcast."""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Callable


class ExecLogger:
    """Manages per-execution and session logging."""

    def __init__(self, log_dir: Path):
        self.log_dir = log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._ws_callbacks: list[Callable[[str], None]] = []
        self._setup_session_logger()

    def _setup_session_logger(self):
        self._session_logger = logging.getLogger("ssh_ops.session")
        self._session_logger.setLevel(logging.DEBUG)
        self._session_logger.handlers.clear()

        # Console handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(logging.Formatter("%(message)s"))
        self._session_logger.addHandler(ch)

        # Session file handler
        session_file = self.log_dir / "session.log"
        fh = logging.FileHandler(session_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s"
        ))
        self._session_logger.addHandler(fh)

    def register_ws_callback(self, callback: Callable[[str], None]):
        """Register WS callback, replacing any previous ones."""
        self._ws_callbacks.clear()
        self._ws_callbacks.append(callback)

    def _broadcast(self, message: str):
        for cb in self._ws_callbacks:
            try:
                cb(message)
            except Exception:
                pass

    def info(self, message: str):
        self._session_logger.info(message)
        self._broadcast(message)

    def error(self, message: str):
        self._session_logger.error(message)
        self._broadcast(f"[ERROR] {message}")

    def debug(self, message: str):
        self._session_logger.debug(message)

    @staticmethod
    def _safe_name(name: str) -> str:
        """Sanitize a name for use as a directory/file component."""
        # Strip path separators and dangerous characters
        safe = name.replace("/", "_").replace("\\", "_").replace("..", "_").replace("\0", "")
        return safe or "unknown"

    def create_exec_log(self, server_name: str, task_name: str) -> Path:
        """Get daily log file for a server. One file per server per day."""
        server_dir = self.log_dir / self._safe_name(server_name)
        server_dir.mkdir(parents=True, exist_ok=True)
        date = datetime.now().strftime("%Y-%m-%d")
        log_path = server_dir / f"{date}.log"
        return log_path

    def write_exec_log(self, log_path: Path, content: str):
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(content)
