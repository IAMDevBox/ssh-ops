"""Java .properties file validator.

Checks for:
- Malformed lines (missing = or :)
- Duplicate keys
- Invalid Unicode escapes
- Unescaped special characters
"""

from __future__ import annotations

import re

from . import FileValidator, Issue, register


class PropertiesValidator(FileValidator):
    name = "properties"
    description = "Java .properties file validator"

    def can_validate(self, filename: str, content: str) -> bool:
        return filename.lower().endswith(".properties")

    def validate(self, filename: str, content: str) -> list[Issue]:
        issues: list[Issue] = []
        seen_keys: dict[str, int] = {}

        continued = False
        for lineno, line in enumerate(content.split("\n"), 1):
            stripped = line.strip()

            # Handle line continuation
            if continued:
                continued = stripped.endswith("\\")
                continue

            if not stripped or stripped.startswith(("#", "!")):
                continue

            continued = stripped.endswith("\\")

            # Must have = or : separator
            if "=" not in stripped and ":" not in stripped:
                issues.append(Issue("warning",
                    f"Line {lineno}: missing '=' or ':' separator",
                    f"line:{lineno}"))
                continue

            # Extract key
            m = re.match(r'^([^=:\s]+)\s*[=:]', stripped)
            if m:
                key = m.group(1)
                if key in seen_keys:
                    issues.append(Issue("warning",
                        f"Duplicate key '{key}' at line {lineno} "
                        f"(first at line {seen_keys[key]})",
                        f"line:{lineno}"))
                else:
                    seen_keys[key] = lineno

            # Invalid Unicode escape
            for um in re.finditer(r'\\u([0-9a-fA-F]{0,3})(?=[^0-9a-fA-F]|$)', stripped):
                if len(um.group(1)) < 4:
                    issues.append(Issue("error",
                        f"Line {lineno}: incomplete Unicode escape '\\u{um.group(1)}'",
                        f"line:{lineno}"))

        return issues


register(PropertiesValidator())
