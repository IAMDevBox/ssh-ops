"""PingGateway (formerly ForgeRock IG) log analyzer.

Parses the pipe-delimited log format:
    timestamp | level | thread | logger | @routeId | message

Groups errors by routeId and matches known IG error patterns.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict

from . import (Finding, LogAnalyzer, aggregate_top_errors, load_knowledge,
               match_patterns, register)

# PingGateway pipe-delimited format
# Example: 2026-03-13T10:15:32,456+00:00 | ERROR | http-nio-8080-exec-3 | ReverseProxyHandler | @my-route | Connection refused
_LINE_RE = re.compile(
    r'^(?P<timestamp>[^|]+)\s*\|\s*(?P<level>\w+)\s*\|\s*(?P<thread>[^|]+)\s*\|'
    r'\s*(?P<logger>[^|]+)\s*\|\s*@?(?P<route>[^|]*)\s*\|\s*(?P<message>.*)$'
)

_ERROR_LEVELS = {"ERROR", "FATAL", "SEVERE"}
_WARN_LEVELS = {"WARN", "WARNING"}


class PingGatewayAnalyzer(LogAnalyzer):
    name = "ping-gateway"
    description = "PingGateway (IG) log analyzer"

    def __init__(self):
        self.patterns = load_knowledge("ping_gateway")

    def can_analyze(self, log_type: str, sample: list[str]) -> bool:
        if log_type == "ping-gateway":
            return True
        # Auto-detect: pipe-delimited with @routeId pattern
        pipe_count = 0
        route_count = 0
        for line in sample:
            if line.count("|") >= 4:
                pipe_count += 1
            if "@" in line and "|" in line:
                route_count += 1
        return pipe_count >= 3 and route_count >= 1

    def analyze(self, lines: list[str]) -> dict:
        total = len(lines)
        error_count = 0
        warning_count = 0
        error_messages: list[str] = []
        by_route: dict[str, dict] = defaultdict(lambda: {"errors": 0, "warnings": 0})
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

            level = m.group("level").strip().upper()
            route = m.group("route").strip() or "system"
            message = m.group("message").strip()

            is_error = level in _ERROR_LEVELS
            is_warning = level in _WARN_LEVELS

            if is_error:
                error_count += 1
                by_route[route]["errors"] += 1
                msg = message[:200] + "..." if len(message) > 200 else message
                error_messages.append((msg, line_num))
            elif is_warning:
                warning_count += 1
                by_route[route]["warnings"] += 1

            # Match knowledge patterns
            matched = match_patterns(message, self.patterns)
            for p in matched:
                cat = p.get("category", "")
                if is_error:
                    by_category[cat]["errors"] = by_category[cat].get("errors", 0) + 1
                elif is_warning:
                    by_category[cat]["warnings"] = by_category[cat].get("warnings", 0) + 1

                key = p["match"]
                if key not in findings_seen:
                    findings_seen.add(key)
                    findings.append(Finding(
                        level=p.get("level", "warning"),
                        message=p.get("description", p["match"]),
                        category=cat,
                        suggestion=p.get("suggestion", ""),
                        line=line_num,
                    ))

        top_errors = aggregate_top_errors(error_messages)

        # Build route summary for output
        route_summary = {}
        for route, counts in sorted(by_route.items()):
            route_summary[route] = counts

        summary_parts = []
        if error_count:
            summary_parts.append(f"{error_count} errors")
        if warning_count:
            summary_parts.append(f"{warning_count} warnings")
        routes_with_errors = [r for r, c in by_route.items() if c["errors"] > 0]
        if routes_with_errors:
            summary_parts.append(f"across routes: {', '.join(routes_with_errors)}")
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
            "by_route": route_summary,
        }


register(PingGatewayAnalyzer())
