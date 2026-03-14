"""Log analyzer framework for remote log analysis.

Each analyzer implements ``can_analyze(log_type, sample)`` to claim a log type,
and ``analyze(lines)`` to return structured analysis results.

Error patterns are loaded from external YAML knowledge files in the
``knowledge/`` subdirectory, so new patterns can be added without code changes.
"""

from __future__ import annotations

import importlib
import logging
import pkgutil
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

_KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"


@dataclass
class Finding:
    """A single analysis finding from a known pattern match."""
    level: str       # "error", "warning", "info"
    message: str
    category: str = ""
    suggestion: str = ""
    count: int = 1
    line: int = 0    # line number where pattern was first seen


class LogAnalyzer:
    """Base class for log analyzers."""

    name: str = "base"
    description: str = ""

    def can_analyze(self, log_type: str, sample: list[str]) -> bool:
        """Return True if this analyzer can handle the given log type/content."""
        return False

    def analyze(self, lines: list[str]) -> dict:
        """Analyze log lines and return structured results.

        Returns:
            {
                "total_lines": int,
                "errors": int,
                "warnings": int,
                "top_errors": [{"pattern": str, "count": int}, ...],
                "findings": [Finding.to_dict(), ...],
                "summary": str,
                "by_category": {category: {"errors": int, "warnings": int}, ...},
            }
        """
        return {"total_lines": len(lines), "errors": 0, "warnings": 0,
                "top_errors": [], "findings": [], "summary": "", "by_category": {}}


def load_knowledge(name: str) -> list[dict]:
    """Load error patterns from analyzers/knowledge/{name}.yml.

    Each pattern dict has: match, regex (bool), level, category,
    description, suggestion.
    """
    path = _KNOWLEDGE_DIR / f"{name}.yml"
    if not path.exists():
        return []
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        patterns = data.get("patterns", []) if data else []
        # Pre-compile regex patterns
        for p in patterns:
            if p.get("regex", False):
                p["_compiled"] = re.compile(p["match"], re.IGNORECASE)
        return patterns
    except Exception as e:
        logger.warning("Failed to load knowledge %s: %s", name, e)
        return []


def match_patterns(line: str, patterns: list[dict]) -> list[dict]:
    """Match a line against a list of knowledge patterns.

    Returns list of matched pattern dicts.
    """
    matched = []
    for p in patterns:
        if p.get("regex", False):
            compiled = p.get("_compiled")
            if compiled and compiled.search(line):
                matched.append(p)
        else:
            if p["match"].lower() in line.lower():
                matched.append(p)
    return matched


def aggregate_top_errors(error_messages: list[tuple[str, int]], top_n: int = 10) -> list[dict]:
    """Count and return the top N most frequent error messages.

    Args:
        error_messages: list of (message, line_number) tuples.
        top_n: max results to return.
    """
    counter: dict[str, dict] = {}
    for msg, line_num in error_messages:
        if msg not in counter:
            counter[msg] = {"count": 0, "lines": []}
        counter[msg]["count"] += 1
        counter[msg]["lines"].append(line_num)
    sorted_errors = sorted(counter.items(), key=lambda x: x[1]["count"], reverse=True)
    return [{"pattern": msg, "count": data["count"], "lines": data["lines"]}
            for msg, data in sorted_errors[:top_n]]


# --- Analyzer Registry ---

_analyzers: list[LogAnalyzer] = []


def register(analyzer: LogAnalyzer):
    """Register a log analyzer."""
    _analyzers.append(analyzer)
    logger.debug("Registered analyzer: %s", analyzer.name)


def get_analyzers() -> list[LogAnalyzer]:
    return list(_analyzers)


def analyze_log(log_type: str, lines: list[str]) -> dict:
    """Run the best matching analyzer against log lines.

    Tries analyzers in order; uses the first that claims the log type.
    Falls back to generic if no specific analyzer matches.
    """
    # Try specific analyzers first
    for analyzer in _analyzers:
        if analyzer.name != "generic" and analyzer.can_analyze(log_type, lines[:20]):
            return analyzer.analyze(lines)
    # Fallback to generic
    for analyzer in _analyzers:
        if analyzer.name == "generic":
            return analyzer.analyze(lines)
    # No analyzers at all
    return {"total_lines": len(lines), "errors": 0, "warnings": 0,
            "top_errors": [], "findings": [], "summary": "No analyzer available",
            "by_category": {}}


def discover_analyzers():
    """Auto-discover and register all analyzers in this package."""
    pkg_path = Path(__file__).parent
    for finder, name, _ in pkgutil.iter_modules([str(pkg_path)]):
        if name.startswith("_"):
            continue
        try:
            importlib.import_module(f"{__package__}.{name}")
        except Exception as e:
            logger.warning("Failed to load analyzer %s: %s", name, e)


# Auto-discover on import
discover_analyzers()
