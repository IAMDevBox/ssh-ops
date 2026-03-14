"""JSON file validator.

Checks uploaded JSON files for:
- Syntax errors (with line number approximation)
- Trailing commas (common copy-paste error)
- Duplicate keys at top level
"""

from __future__ import annotations

import json
import re

from . import FileValidator, Issue, register


class JsonValidator(FileValidator):
    name = "json"
    description = "JSON syntax validator"

    def can_validate(self, filename: str, content: str) -> bool:
        return filename.lower().endswith(".json")

    def validate(self, filename: str, content: str) -> list[Issue]:
        issues: list[Issue] = []

        # --- Trailing comma detection (before parse, since it causes parse failure) ---
        # Match ,\s*} or ,\s*]
        for lineno, line in enumerate(content.split("\n"), 1):
            stripped = line.rstrip()
            if re.search(r',\s*$', stripped):
                # Check if next non-empty line starts with } or ]
                remaining = content.split("\n")[lineno:]
                for next_line in remaining:
                    ns = next_line.strip()
                    if not ns:
                        continue
                    if ns[0] in "}]":
                        issues.append(Issue("error",
                            f"Line {lineno}: trailing comma before closing bracket",
                            f"line:{lineno}"))
                    break

        # --- JSON parse ---
        try:
            json.loads(content)
        except json.JSONDecodeError as e:
            issues.append(Issue("error",
                f"JSON syntax error at line {e.lineno}, col {e.colno}: {e.msg}",
                f"line:{e.lineno}"))
            return issues

        # --- Duplicate keys (top-level) ---
        seen = {}
        for lineno, line in enumerate(content.split("\n"), 1):
            m = re.match(r'^\s*"([^"]+)"\s*:', line)
            if m:
                key = m.group(1)
                indent = len(line) - len(line.lstrip())
                if indent <= 2:  # top-level keys
                    if key in seen:
                        issues.append(Issue("warning",
                            f"Duplicate key '{key}' at line {lineno} "
                            f"(first at line {seen[key]}) — last value wins",
                            f"line:{lineno}"))
                    else:
                        seen[key] = lineno

        return issues


register(JsonValidator())
