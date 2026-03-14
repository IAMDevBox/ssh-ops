"""Shell script validator.

Checks uploaded shell scripts (.sh, .bash) for common issues:
- Missing shebang line
- Missing 'set -e' (or set -euo pipefail)
- Unquoted variable expansions in dangerous commands (rm, mv, cp)
- Hardcoded passwords / secrets
- Common bash pitfalls
"""

from __future__ import annotations

import re

from . import FileValidator, Issue, register

# Commands where unquoted $VAR is dangerous
_DANGEROUS_CMDS = {"rm", "mv", "cp", "chmod", "chown", "rsync", "find"}

# Patterns that suggest hardcoded secrets in scripts
_SECRET_PATTERNS = [
    (r'(?:PASSWORD|PASSWD|SECRET|API_KEY|TOKEN)\s*=\s*["\'][^$][^"\']{4,}["\']',
     "Hardcoded secret in variable assignment"),
    (r'curl\s.*-[uU]\s+\S+:\S+', "Hardcoded credentials in curl command"),
]


class ShellScriptValidator(FileValidator):
    name = "shell-script"
    description = "Shell script best-practice validator"

    def can_validate(self, filename: str, content: str) -> bool:
        lower = filename.lower()
        if lower.endswith((".sh", ".bash")):
            return True
        # Check shebang for files without extension
        if content.startswith("#!/"):
            first_line = content.split("\n", 1)[0]
            if "sh" in first_line or "bash" in first_line:
                return True
        return False

    def validate(self, filename: str, content: str) -> list[Issue]:
        issues: list[Issue] = []
        lines = content.split("\n")

        # --- Shebang ---
        first_line = lines[0].strip()
        if not first_line.startswith("#!"):
            issues.append(Issue("warning",
                "Missing shebang line (e.g. #!/bin/bash) — script may use wrong interpreter",
                "shebang"))
        elif "/env " not in first_line and "/bash" not in first_line and "/sh" not in first_line:
            issues.append(Issue("warning",
                f"Unusual shebang: {first_line}", "shebang"))

        # --- set -e / errexit ---
        has_set_e = False
        for line in lines[:30]:  # check first 30 lines
            stripped = line.strip()
            if re.match(r'^set\s+.*-[a-zA-Z]*e', stripped):
                has_set_e = True
                break
        if not has_set_e:
            issues.append(Issue("info",
                "No 'set -e' found — script will continue after errors. "
                "Consider 'set -euo pipefail' for safety.",
                "errexit"))

        # --- Dangerous unquoted variables ---
        for lineno, line in enumerate(lines, 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            for cmd in _DANGEROUS_CMDS:
                # Match: rm -rf $VAR/ or rm $SOMEDIR (unquoted)
                pattern = r'\b' + cmd + r'\b.*\s\$\w+'
                if re.search(pattern, stripped):
                    # Check it's not inside quotes
                    # Simple heuristic: if $VAR appears outside "..." it's unquoted
                    if not re.search(r'"[^"]*\$\w+[^"]*"', stripped):
                        issues.append(Issue("warning",
                            f"Line {lineno}: unquoted variable in '{cmd}' command — "
                            f"use quotes to prevent word splitting",
                            f"line:{lineno}"))
                        break  # one warning per line

        # --- Secrets ---
        for pattern, msg in _SECRET_PATTERNS:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                issues.append(Issue("warning",
                    f"{msg} — use environment variables or a secrets file",
                    "security"))

        # --- Common pitfalls ---
        for lineno, line in enumerate(lines, 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            # cd without || exit
            if re.match(r'^cd\s+\S+\s*$', stripped) and "||" not in stripped:
                issues.append(Issue("info",
                    f"Line {lineno}: 'cd' without error handling — "
                    f"use 'cd dir || exit 1' to fail safely",
                    f"line:{lineno}"))

        return issues


register(ShellScriptValidator())
