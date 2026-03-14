"""PingDS (formerly ForgeRock DS / OpenDJ) log analyzer.

Parses the DS log format:
    [timestamp] category=X severity=Y msgID=Z msg=message

Groups errors by category (CORE, SYNC, BACKEND, ACCESS_CONTROL, etc.).
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict

from . import (Finding, LogAnalyzer, aggregate_top_errors, group_stack_traces, load_knowledge,
               match_patterns, register)

# DS log line pattern
# [13/Mar/2026:10:15:32.456 +0000] category=CORE severity=ERROR msgID=8389734 msg=Disk space low
_LINE_RE = re.compile(
    r'\[(?P<timestamp>[^\]]+)\]\s+'
    r'category=(?P<category>\w+)\s+'
    r'severity=(?P<severity>\w+)\s+'
    r'msgID=(?P<msgid>\d+)\s+'
    r'msg=(?P<message>.*)'
)

_ERROR_SEVERITIES = {"ERROR", "FATAL"}
_WARN_SEVERITIES = {"WARNING", "WARN"}

# Category descriptions
_CATEGORY_LABELS = {
    "CORE": "Core Server",
    "SYNC": "Replication",
    "REPLICATION": "Replication",
    "BACKEND": "Database Backend",
    "ACCESS_CONTROL": "Access Control (ACI)",
    "SCHEMA": "Schema Validation",
    "PLUGIN": "Server Plugin",
    "PROTOCOL": "LDAP Protocol",
    "EXTENSIONS": "Extensions",
    "CONFIG": "Configuration",
    "ADMIN": "Administration",
    "RUNTIME_INFORMATION": "Runtime Info",
    "UTIL": "Utilities",
    "LOG": "Logging",
    "JEB": "JE Backend",
    "TOOLS": "CLI Tools",
}


class PingDSAnalyzer(LogAnalyzer):
    name = "ping-ds"
    description = "PingDS (Directory Server) log analyzer"

    def __init__(self):
        self.patterns = load_knowledge("ping_ds")

    def can_analyze(self, log_type: str, sample: list[str]) -> bool:
        if log_type == "ping-ds":
            return True
        # Auto-detect: category= severity= msgID= pattern
        match_count = 0
        for line in sample:
            if "category=" in line and "severity=" in line and "msgID=" in line:
                match_count += 1
        return match_count >= 2

    def analyze(self, lines: list[str]) -> dict:
        total = len(lines)
        lines = group_stack_traces(lines)
        error_count = 0
        warning_count = 0
        error_messages: list[str] = []
        by_category: dict[str, dict] = defaultdict(lambda: {"errors": 0, "warnings": 0})
        findings: list[Finding] = []
        findings_seen: set[str] = set()

        for line_num, line in enumerate(lines, 1):
            stripped = line.strip()
            if not stripped:
                continue

            m = _LINE_RE.match(stripped)
            if not m:
                continue

            category = m.group("category").upper()
            severity = m.group("severity").upper()
            message = m.group("message").strip()
            msgid = m.group("msgid")

            is_error = severity in _ERROR_SEVERITIES
            is_warning = severity in _WARN_SEVERITIES

            if is_error:
                error_count += 1
                by_category[category]["errors"] += 1
                msg = f"[msgID={msgid}] {message}"
                msg = msg[:200] + "..." if len(msg) > 200 else msg
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
                        category=p.get("category", category.lower()),
                        suggestion=p.get("suggestion", ""),
                        line=line_num,
                    ))

        top_errors = aggregate_top_errors(error_messages)

        # Add category labels to output
        labeled_categories = {}
        for cat, counts in by_category.items():
            label = _CATEGORY_LABELS.get(cat, cat)
            labeled_categories[cat] = {**counts, "label": label}

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
            "by_category": labeled_categories,
        }


register(PingDSAnalyzer())
