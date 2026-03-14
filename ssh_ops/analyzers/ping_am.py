"""PingAM (formerly ForgeRock AM) log analyzer.

Supports two log formats:
1. JSON (recommended): {"timestamp", "level", "thread", "mdc": {"transactionId"}, "logger", "message"}
2. Legacy text: amCategory:date: Thread[...]\nmessage

Groups errors by logger category (authentication, session, federation, oauth2, etc.).
"""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict

from . import (Finding, LogAnalyzer, aggregate_top_errors, group_stack_traces, load_knowledge,
               match_patterns, register)

# Logger category mapping
_CATEGORIES = {
    "authentication": "authentication",
    "auth": "authentication",
    "core": "core",
    "coresystem": "core",
    "federation": "federation",
    "saml": "federation",
    "idrepo": "datastore",
    "identity": "datastore",
    "oauth2": "oauth2",
    "oidc": "oauth2",
    "session": "session",
    "cts": "session",
    "policy": "authorization",
}

_ERROR_LEVELS = {"ERROR", "SEVERE", "FATAL"}
_WARN_LEVELS = {"WARN", "WARNING"}

# Legacy text format header: amCategory:date: Thread[...]
_LEGACY_HEADER_RE = re.compile(
    r'^(am\w+):(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}:\d{3}\s+\w+\s+\w+):\s+Thread\[([^\]]+)\]'
)


def _classify_logger(logger_name: str) -> str:
    """Map a logger name to a category."""
    lower = logger_name.lower()
    for keyword, category in _CATEGORIES.items():
        if keyword in lower:
            return category
    return "other"


class PingAMAnalyzer(LogAnalyzer):
    name = "ping-am"
    description = "PingAM (Access Management) log analyzer"

    def __init__(self):
        self.patterns = load_knowledge("ping_am")

    def can_analyze(self, log_type: str, sample: list[str]) -> bool:
        if log_type == "ping-am":
            return True
        # Auto-detect JSON with openam logger
        for line in sample:
            line = line.strip()
            if not line:
                continue
            if line.startswith("{"):
                try:
                    obj = json.loads(line)
                    logger = obj.get("logger", "")
                    if "openam" in logger.lower() or "forgerock.openam" in logger.lower():
                        return True
                except (json.JSONDecodeError, ValueError):
                    pass
            # Check legacy format
            if _LEGACY_HEADER_RE.match(line):
                return True
        return False

    def analyze(self, lines: list[str]) -> dict:
        total = len(lines)
        lines = group_stack_traces(lines)
        # Detect format from first non-empty line
        is_json = False
        for line in lines:
            stripped = line.strip()
            if stripped:
                is_json = stripped.startswith("{")
                break

        if is_json:
            return self._analyze_json(lines, total)
        return self._analyze_text(lines, total)

    def _analyze_json(self, lines: list[str], total: int) -> dict:
        error_count = 0
        warning_count = 0
        error_messages: list[tuple[str, int]] = []
        by_category: dict[str, dict] = defaultdict(lambda: {"errors": 0, "warnings": 0})
        findings: list[Finding] = []
        findings_seen: set[str] = set()

        for line_num, line in enumerate(lines, 1):
            stripped = line.strip()
            if not stripped or not stripped.startswith("{"):
                continue
            try:
                obj = json.loads(stripped)
            except (json.JSONDecodeError, ValueError):
                continue

            level = str(obj.get("level", "")).upper()
            logger_name = obj.get("logger", "")
            message = obj.get("message", "")
            category = _classify_logger(logger_name)

            is_error = level in _ERROR_LEVELS
            is_warning = level in _WARN_LEVELS

            if is_error:
                error_count += 1
                by_category[category]["errors"] += 1
                msg = message[:200] + "..." if len(message) > 200 else message
                error_messages.append((msg, line_num))
            elif is_warning:
                warning_count += 1
                by_category[category]["warnings"] += 1

            # Match knowledge patterns
            matched = match_patterns(message, self.patterns)
            for p in matched:
                key = p["match"]
                if key not in findings_seen:
                    findings_seen.add(key)
                    findings.append(Finding(
                        level=p.get("level", "warning"),
                        message=p.get("description", p["match"]),
                        category=p.get("category", category),
                        suggestion=p.get("suggestion", ""),
                        line=line_num,
                    ))

        return self._build_result(total, error_count, warning_count,
                                   error_messages, by_category, findings)

    def _analyze_text(self, lines: list[str], total: int) -> dict:
        error_count = 0
        warning_count = 0
        error_messages: list[tuple[str, int]] = []
        by_category: dict[str, dict] = defaultdict(lambda: {"errors": 0, "warnings": 0})
        findings: list[Finding] = []
        findings_seen: set[str] = set()

        current_category = "other"
        for line_num, line in enumerate(lines, 1):
            stripped = line.strip()
            if not stripped:
                continue

            # Check for header line
            header = _LEGACY_HEADER_RE.match(stripped)
            if header:
                am_category = header.group(1)  # e.g. amAuthentication
                current_category = _classify_logger(am_category)
                continue

            # Check for severity in content lines
            level_upper = stripped.split(":", 1)[0].strip().upper() if ":" in stripped else ""
            is_error = level_upper in _ERROR_LEVELS or "ERROR" in stripped.upper()
            is_warning = level_upper in _WARN_LEVELS or "WARNING" in stripped.upper()

            if is_error:
                error_count += 1
                by_category[current_category]["errors"] += 1
                msg = stripped[:200] + "..." if len(stripped) > 200 else stripped
                error_messages.append((msg, line_num))
            elif is_warning:
                warning_count += 1
                by_category[current_category]["warnings"] += 1

            # Match knowledge patterns
            matched = match_patterns(stripped, self.patterns)
            for p in matched:
                key = p["match"]
                if key not in findings_seen:
                    findings_seen.add(key)
                    findings.append(Finding(
                        level=p.get("level", "warning"),
                        message=p.get("description", p["match"]),
                        category=p.get("category", current_category),
                        suggestion=p.get("suggestion", ""),
                        line=line_num,
                    ))

        return self._build_result(total, error_count, warning_count,
                                   error_messages, by_category, findings)

    def _build_result(self, total, error_count, warning_count,
                      error_messages, by_category, findings):
        top_errors = aggregate_top_errors(error_messages)

        summary_parts = []
        if error_count:
            summary_parts.append(f"{error_count} errors")
        if warning_count:
            summary_parts.append(f"{warning_count} warnings")
        cats_with_errors = [c for c, v in by_category.items() if v["errors"] > 0]
        if cats_with_errors:
            summary_parts.append(f"categories: {', '.join(cats_with_errors)}")
        summary = f"{', '.join(summary_parts) or 'No issues'} in {total} lines"

        return {
            "total_lines": total,
            "errors": error_count,
            "warnings": warning_count,
            "top_errors": top_errors,
            "findings": [{"level": f.level, "message": f.message,
                          "category": f.category, "suggestion": f.suggestion,
                          "line": f.line}
                         for f in findings],
            "summary": summary,
            "by_category": dict(by_category),
        }


register(PingAMAnalyzer())
