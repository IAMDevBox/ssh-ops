"""Generic log analyzer — fallback for any log type.

Detects common error/warning patterns (ERROR, WARN, FATAL, Exception, CRITICAL)
and aggregates them by frequency. Also matches patterns from knowledge/generic.yml.
"""

from __future__ import annotations

import re
from collections import Counter

from . import (Finding, LogAnalyzer, aggregate_top_errors, group_stack_traces,
               load_knowledge, match_patterns, register)

# Regex to strip common timestamp prefixes for error grouping
_TIMESTAMP_RE = re.compile(
    r'^[\[\(]?\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2}[^\]]*[\]\)]?\s*[-|]?\s*'
)

# Severity detection patterns
_SEVERITY_PATTERNS = {
    "error": re.compile(r'\b(ERROR|SEVERE|FATAL|CRITICAL)\b', re.IGNORECASE),
    "warning": re.compile(r'\b(WARN|WARNING)\b', re.IGNORECASE),
}


class GenericAnalyzer(LogAnalyzer):
    name = "generic"
    description = "Generic error pattern analyzer (fallback)"

    def __init__(self):
        self.patterns = load_knowledge("generic")

    def can_analyze(self, log_type: str, sample: list[str]) -> bool:
        return log_type == "generic" or log_type == ""

    def analyze(self, lines: list[str]) -> dict:
        total = len(lines)
        # Merge stack traces before analysis
        lines = group_stack_traces(lines)
        error_count = 0
        warning_count = 0
        error_messages: list[str] = []
        findings: list[Finding] = []
        findings_seen: set[str] = set()

        for line_num, line in enumerate(lines, 1):
            stripped = line.strip()
            if not stripped:
                continue

            # Detect severity
            is_error = bool(_SEVERITY_PATTERNS["error"].search(stripped))
            is_warning = bool(_SEVERITY_PATTERNS["warning"].search(stripped))

            if is_error:
                error_count += 1
                # Strip timestamp for grouping
                msg = _TIMESTAMP_RE.sub("", stripped).strip()
                # Truncate long messages
                if len(msg) > 200:
                    msg = msg[:200] + "..."
                error_messages.append((msg, line_num))
            elif is_warning:
                warning_count += 1

            # Match knowledge patterns
            matched = match_patterns(stripped, self.patterns)
            for p in matched:
                key = p["match"]
                if key not in findings_seen:
                    findings_seen.add(key)
                    findings.append(Finding(
                        level=p.get("level", "warning"),
                        message=p.get("description", p["match"]),
                        category=p.get("category", ""),
                        suggestion=p.get("suggestion", ""),
                        line=line_num,
                    ))

        top_errors = aggregate_top_errors(error_messages)

        summary_parts = []
        if error_count:
            summary_parts.append(f"{error_count} errors")
        if warning_count:
            summary_parts.append(f"{warning_count} warnings")
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
            "by_category": {},
        }


register(GenericAnalyzer())
