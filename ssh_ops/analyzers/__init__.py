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


# --- Stack Trace Grouping ---

# Lines that belong to a stack trace (continuation of an exception)
_TRACE_LINE_RE = re.compile(r'^\s+(at |\.{3}\s+\d+\s+more)|^Caused by[: ]')
_EXCEPTION_RE = re.compile(
    r'(?:Exception|Error|Throwable|Fault)[\s:$]', re.IGNORECASE
)


def group_stack_traces(lines: list[str]) -> list[str]:
    """Merge multi-line Java stack traces into single logical lines.

    Input:
        ERROR NullPointerException: foo
          at com.example.A.method(A.java:10)
          at com.example.B.call(B.java:20)
        Caused by: IllegalStateException: bar
          at com.example.C.init(C.java:5)
        INFO normal line

    Output:
        ERROR NullPointerException: foo | at com.example.A.method(A.java:10) | Caused by: IllegalStateException: bar | at com.example.C.init(C.java:5)
        INFO normal line

    The merged line keeps the original exception as the primary text,
    with stack frames joined by ' | ' for compact display.
    """
    result: list[str] = []
    current_block: list[str] = []

    def _flush():
        if not current_block:
            return
        # First line is the exception, rest are stack trace / Caused by
        merged = current_block[0]
        # Keep only first 2 stack frames + root cause for brevity
        frames = []
        root_cause = ""
        for sl in current_block[1:]:
            stripped = sl.strip()
            if stripped.startswith("Caused by"):
                root_cause = stripped
                frames = []  # reset frames — collect from root cause
            elif stripped.startswith("at ") and len(frames) < 5:
                frames.append(stripped)
        if root_cause:
            merged += " | " + root_cause
        if frames:
            merged += " | " + " | ".join(frames)
        result.append(merged)

    for line in lines:
        if _TRACE_LINE_RE.match(line):
            # Stack trace continuation
            if current_block:
                current_block.append(line)
            else:
                # Orphan stack line — keep as-is
                result.append(line)
        else:
            # New line — flush any pending block
            _flush()
            current_block = []
            # Check if this line starts a new exception
            if _EXCEPTION_RE.search(line):
                current_block = [line]
            else:
                result.append(line)
    _flush()
    return result


def extract_root_cause(merged_line: str) -> str:
    """Extract the root cause from a merged stack trace line.

    Returns the innermost 'Caused by: ...' or the exception line itself.
    """
    # Find last "Caused by:" segment
    parts = merged_line.split(" | ")
    for part in reversed(parts):
        stripped = part.strip()
        if stripped.startswith("Caused by"):
            # Extract just the exception class and message
            return stripped
    # No Caused by — return the first line (the exception itself)
    return parts[0] if parts else merged_line


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
