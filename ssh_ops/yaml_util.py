"""Round-trip YAML helper — preserves comments and formatting using ruamel.yaml."""

from io import StringIO
from pathlib import Path

from ruamel.yaml import YAML

_yaml_rt = YAML(typ="rt")
_yaml_rt.default_flow_style = False
_yaml_rt.allow_unicode = True
_yaml_rt.preserve_quotes = True

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


def dump_yaml_str(data) -> str:
    """Dump data to a YAML string (for display purposes, no comment preservation needed)."""
    buf = StringIO()
    _yaml_rt.dump(data, buf)
    return buf.getvalue()


def safe_load_str(text: str):
    """Parse YAML text with safe loader (for validation)."""
    return _yaml_safe.load(text)
