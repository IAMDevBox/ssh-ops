"""Plugin system for file validation before upload.

Each plugin implements ``can_validate(filename, content)`` to claim a file,
and ``validate(content)`` to return a list of issues.
"""

from __future__ import annotations

import importlib
import logging
import pkgutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class Issue:
    """A single validation issue."""
    level: str  # "error" or "warning"
    message: str
    field: str = ""  # optional JSON path or field name

    def to_dict(self) -> dict:
        d = {"level": self.level, "message": self.message}
        if self.field:
            d["field"] = self.field
        return d


class FileValidator:
    """Base class for file validation plugins."""

    name: str = "base"
    description: str = ""

    def can_validate(self, filename: str, content: str) -> bool:
        """Return True if this plugin can validate the given file."""
        return False

    def validate(self, filename: str, content: str) -> list[Issue]:
        """Validate file content. Return list of Issues (errors/warnings)."""
        return []


# --- Plugin Registry ---

_plugins: list[FileValidator] = []


def register(plugin: FileValidator):
    """Register a validator plugin."""
    _plugins.append(plugin)
    logger.debug("Registered plugin: %s", plugin.name)


def get_plugins() -> list[FileValidator]:
    return list(_plugins)


def validate_file(filename: str, content: str) -> Optional[list[dict]]:
    """Run all matching plugins against a file.

    Returns None if no plugin matches, or a list of result dicts:
    [{"plugin": "name", "description": "...", "errors": [...], "warnings": [...]}, ...]
    """
    results = []
    for plugin in _plugins:
        if plugin.can_validate(filename, content):
            issues = plugin.validate(filename, content)
            errors = [i.to_dict() for i in issues if i.level == "error"]
            warnings = [i.to_dict() for i in issues if i.level == "warning"]
            results.append({
                "plugin": plugin.name,
                "description": plugin.description,
                "errors": errors,
                "warnings": warnings,
            })
    return results or None


def discover_plugins():
    """Auto-discover and register all plugins in this package."""
    pkg_path = Path(__file__).parent
    for finder, name, _ in pkgutil.iter_modules([str(pkg_path)]):
        if name.startswith("_"):
            continue
        try:
            importlib.import_module(f"{__package__}.{name}")
        except Exception as e:
            logger.warning("Failed to load plugin %s: %s", name, e)


# Auto-discover on import
discover_plugins()
