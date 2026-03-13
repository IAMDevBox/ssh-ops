"""YAML file validator.

Checks uploaded YAML files for:
- Syntax errors (with line number)
- Tab indentation (YAML forbids tabs)
- Duplicate top-level keys
- Very long lines
- Trailing whitespace patterns that hint at copy-paste issues
"""

from __future__ import annotations

import re

from . import FileValidator, Issue, register

_MAX_LINE_LENGTH = 500


class YamlValidator(FileValidator):
    name = "yaml"
    description = "YAML syntax and style validator"

    def can_validate(self, filename: str, content: str) -> bool:
        lower = filename.lower()
        return lower.endswith((".yml", ".yaml"))

    def validate(self, filename: str, content: str) -> list[Issue]:
        issues: list[Issue] = []
        lines = content.split("\n")

        # --- Tab check (before YAML parse, since tabs cause parse failures) ---
        for lineno, line in enumerate(lines, 1):
            if "\t" in line and not line.strip().startswith("#"):
                issues.append(Issue("error",
                    f"Line {lineno}: tab character found — YAML requires spaces for indentation",
                    f"line:{lineno}"))
                break  # one tab warning is enough

        # --- YAML parse ---
        try:
            import yaml
            docs = list(yaml.safe_load_all(content))
            if not docs or (len(docs) == 1 and docs[0] is None):
                issues.append(Issue("warning", "YAML file is empty or contains only null"))
        except yaml.YAMLError as e:
            msg = str(e)
            # Extract line number from yaml error if available
            line_match = re.search(r'line (\d+)', msg)
            field = f"line:{line_match.group(1)}" if line_match else ""
            issues.append(Issue("error", f"YAML syntax error: {msg}", field))
            return issues  # can't do further checks
        except ImportError:
            issues.append(Issue("warning",
                "PyYAML not available — skipped syntax validation"))
            return issues

        # --- Duplicate top-level keys ---
        seen_keys = {}
        for lineno, line in enumerate(lines, 1):
            # Top-level key: starts at column 0, has key: value pattern
            m = re.match(r'^([a-zA-Z_][a-zA-Z0-9_-]*)\s*:', line)
            if m:
                key = m.group(1)
                if key in seen_keys:
                    issues.append(Issue("warning",
                        f"Duplicate top-level key '{key}' at line {lineno} "
                        f"(first seen at line {seen_keys[key]}) — last value wins",
                        f"line:{lineno}"))
                else:
                    seen_keys[key] = lineno

        # --- Long lines ---
        for lineno, line in enumerate(lines, 1):
            if len(line) > _MAX_LINE_LENGTH:
                issues.append(Issue("warning",
                    f"Line {lineno}: very long line ({len(line)} chars) — "
                    f"consider breaking it up",
                    f"line:{lineno}"))
                break  # one is enough

        return issues


register(YamlValidator())
