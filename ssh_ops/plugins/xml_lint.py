"""XML file validator.

Checks uploaded XML files for:
- Well-formedness errors (with line number)
- Missing XML declaration
- Unclosed tags
"""

from __future__ import annotations

import re
from xml.etree import ElementTree

from . import FileValidator, Issue, register


class XmlValidator(FileValidator):
    name = "xml"
    description = "XML syntax validator"

    def can_validate(self, filename: str, content: str) -> bool:
        lower = filename.lower()
        return lower.endswith((".xml", ".xsl", ".xslt", ".xsd", ".svg", ".plist"))

    def validate(self, filename: str, content: str) -> list[Issue]:
        issues: list[Issue] = []

        if not content.strip():
            issues.append(Issue("warning", "XML file is empty"))
            return issues

        # --- XML declaration ---
        if not content.lstrip().startswith("<?xml"):
            issues.append(Issue("warning",
                "Missing XML declaration (<?xml version=\"1.0\"?>) — "
                "encoding may not be detected correctly"))

        # --- Parse ---
        try:
            ElementTree.fromstring(content)
        except ElementTree.ParseError as e:
            msg = str(e)
            line_match = re.search(r'line (\d+)', msg)
            col_match = re.search(r'column (\d+)', msg)
            field = f"line:{line_match.group(1)}" if line_match else ""
            location = ""
            if line_match:
                location = f" at line {line_match.group(1)}"
                if col_match:
                    location += f", col {col_match.group(1)}"
            issues.append(Issue("error", f"XML parse error{location}: {msg}", field))

        return issues


register(XmlValidator())
