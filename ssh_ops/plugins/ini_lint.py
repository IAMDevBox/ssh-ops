"""INI/conf file validator.

Checks uploaded INI-style config files for:
- Syntax errors (malformed lines)
- Duplicate sections
- Duplicate keys within a section
"""

from __future__ import annotations

import configparser
import io
import re

from . import FileValidator, Issue, register


class IniValidator(FileValidator):
    name = "ini"
    description = "INI/conf syntax validator"

    def can_validate(self, filename: str, content: str) -> bool:
        lower = filename.lower()
        if lower.endswith((".ini", ".cfg", ".cnf")):
            return True
        if lower.endswith(".conf"):
            # Skip if it looks like nginx/apache config (has braces or directive patterns)
            if re.search(r'\{|\bserver\b|\blocation\b|\bproxy_pass\b', content):
                return False
            return True
        return False

    def validate(self, filename: str, content: str) -> list[Issue]:
        issues: list[Issue] = []

        if not content.strip():
            issues.append(Issue("warning", "Config file is empty"))
            return issues

        # --- Parse with configparser ---
        parser = configparser.ConfigParser(strict=True)
        try:
            parser.read_string(content)
        except configparser.DuplicateSectionError as e:
            issues.append(Issue("warning",
                f"Duplicate section [{e.section}] at line {e.lineno}",
                f"line:{e.lineno}"))
        except configparser.DuplicateOptionError as e:
            issues.append(Issue("warning",
                f"Duplicate key '{e.option}' in [{e.section}] at line {e.lineno}",
                f"line:{e.lineno}"))
        except configparser.MissingSectionHeaderError as e:
            issues.append(Issue("error",
                f"Line {e.lineno}: content before first section header — "
                f"add a [section] header first",
                f"line:{e.lineno}"))
        except configparser.ParsingError as e:
            msg = str(e).split("\n")[0]
            issues.append(Issue("error", f"INI parse error: {msg}"))

        # --- Malformed lines (not key=val, not section, not comment, not empty) ---
        in_section = False
        for lineno, line in enumerate(content.split("\n"), 1):
            stripped = line.strip()
            if not stripped or stripped.startswith(("#", ";")):
                continue
            if re.match(r'^\[.+\]$', stripped):
                in_section = True
                continue
            if in_section and "=" not in stripped and ":" not in stripped:
                issues.append(Issue("warning",
                    f"Line {lineno}: '{stripped[:40]}' — not a key=value pair",
                    f"line:{lineno}"))

        return issues


register(IniValidator())
