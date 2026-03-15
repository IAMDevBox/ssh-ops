"""Round-trip YAML helper — preserves comments and formatting using ruamel.yaml."""

import threading
from contextlib import contextmanager
from io import StringIO
from pathlib import Path

from ruamel.yaml import YAML

# File-level locks to prevent concurrent read-modify-write on the same config file
_file_locks: dict[str, threading.Lock] = {}
_locks_lock = threading.Lock()


def _get_file_lock(path: str | Path) -> threading.Lock:
    """Get a per-file lock for safe concurrent access."""
    key = str(Path(path).resolve())
    with _locks_lock:
        if key not in _file_locks:
            _file_locks[key] = threading.Lock()
        return _file_locks[key]


_yaml_rt = YAML(typ="rt")
_yaml_rt.default_flow_style = False
_yaml_rt.allow_unicode = True
_yaml_rt.preserve_quotes = True
_yaml_rt.width = 4096  # prevent long strings (e.g. ENC() passwords) from wrapping

# Plain safe loader for validation of user-submitted text
_yaml_safe = YAML(typ="safe")


def load_yaml(path: str | Path):
    """Load a YAML file with round-trip parsing (preserves comments)."""
    with open(path, "r", encoding="utf-8") as f:
        return _yaml_rt.load(f)


def dump_yaml(data, path: str | Path):
    """Write data to a YAML file, preserving comments if data came from load_yaml."""
    with open(path, "w", encoding="utf-8") as f:
        _yaml_rt.dump(data, f)


@contextmanager
def yaml_transaction(path: str | Path):
    """Context manager for atomic read-modify-write on a YAML file.

    Usage:
        with yaml_transaction(config_path) as raw:
            raw["key"] = "value"
        # file is automatically saved on exit
    """
    lock = _get_file_lock(path)
    with lock:
        p = Path(path)
        raw = load_yaml(p) if p.exists() else {}
        if raw is None:
            raw = {}
        yield raw
        dump_yaml(raw, p)


def dump_yaml_str(data) -> str:
    """Dump data to a YAML string (for display purposes, no comment preservation needed)."""
    buf = StringIO()
    _yaml_rt.dump(data, buf)
    return buf.getvalue()


def safe_load_str(text: str):
    """Parse YAML text with safe loader (for validation)."""
    return _yaml_safe.load(text)
