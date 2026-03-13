"""PingGateway (ForgeRock IG) route file validator.

Validates JSON route files before upload to ensure they follow
PingGateway best practices and won't cause runtime errors.

Route file structure:
  {
    "name": "my-route",
    "baseURI": "https://backend.example.com:8443",
    "condition": "${matches(request.uri.path, '^/api')}",
    "handler": { "type": "Chain", "config": { ... } }
  }
"""

from __future__ import annotations

import json
import re
from urllib.parse import urlparse

from . import FileValidator, Issue, register

# Known handler types in PingGateway
_KNOWN_HANDLER_TYPES = {
    "Chain", "ClientHandler", "DispatchHandler",
    "ReverseProxyHandler", "Router", "ScriptableHandler",
    "SequenceHandler", "StaticResponseHandler", "SwitchHandler",
    "ResourceHandler",
}

# Known filter types
_KNOWN_FILTER_TYPES = {
    "AssignmentFilter", "CaptureDecorator", "Chain",
    "ConditionalFilter", "CookieFilter", "CorsFilter",
    "CryptoHeaderFilter", "EntityExtractFilter", "HeaderFilter",
    "HttpBasicAuthFilter", "JwtValidationFilter", "LocationHeaderFilter",
    "OAuth2ClientFilter", "OAuth2ResourceServerFilter",
    "PasswordReplayFilter", "PolicyEnforcementFilter",
    "ScriptableFilter", "SingleSignOnFilter", "SqlAttributesFilter",
    "StaticRequestFilter", "SwitchFilter", "ThrottlingFilter",
    "TokenTransformationFilter", "UriRoutingFilter",
    "AmSessionIdleTimeoutFilter",
}

# Patterns that suggest hardcoded secrets
_SECRET_PATTERNS = [
    (r'"password"\s*:\s*"[^$][^"]{4,}"', "Hardcoded password detected"),
    (r'"client_secret"\s*:\s*"[^$][^"]{4,}"', "Hardcoded client_secret detected"),
    (r'"sharedSecret"\s*:\s*"[^$][^"]{4,}"', "Hardcoded sharedSecret detected"),
    (r'"api[_-]?key"\s*:\s*"[^$][^"]{4,}"', "Hardcoded API key detected"),
]


class PingGatewayValidator(FileValidator):
    name = "ping-gateway"
    description = "PingGateway / ForgeRock IG route file validator"

    def can_validate(self, filename: str, content: str) -> bool:
        # Match .json files that look like PingGateway routes
        if not filename.lower().endswith(".json"):
            return False
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, ValueError):
            return False
        if not isinstance(data, dict):
            return False
        # Must have "handler" plus at least one of baseURI/condition
        keys = set(data.keys())
        if "handler" not in keys:
            return False
        return bool({"baseURI", "condition"} & keys)

    def validate(self, filename: str, content: str) -> list[Issue]:
        issues: list[Issue] = []

        # --- JSON parse ---
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            issues.append(Issue("error", f"Invalid JSON: {e}"))
            return issues

        if not isinstance(data, dict):
            issues.append(Issue("error", "Route must be a JSON object"))
            return issues

        # --- name ---
        name = data.get("name")
        if not name:
            issues.append(Issue("warning", "Missing 'name' field — route will use filename as name", "name"))
        elif not isinstance(name, str):
            issues.append(Issue("error", "'name' must be a string", "name"))
        elif not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9._-]*$', name):
            issues.append(Issue("warning",
                f"Route name '{name}' contains unusual characters — use alphanumeric, dot, hyphen, underscore",
                "name"))

        # --- baseURI ---
        base_uri = data.get("baseURI")
        if base_uri is not None:
            if not isinstance(base_uri, str):
                issues.append(Issue("error", "'baseURI' must be a string", "baseURI"))
            elif base_uri.startswith("${"):
                pass  # expression — can't validate statically
            else:
                issues.extend(self._validate_base_uri(base_uri))

        # --- condition ---
        condition = data.get("condition")
        if condition is None:
            issues.append(Issue("warning",
                "No 'condition' defined — route matches ALL requests. "
                "Consider adding a path condition.", "condition"))
        elif isinstance(condition, str):
            issues.extend(self._validate_condition(condition))
        elif isinstance(condition, dict):
            cond_expr = condition.get("condition")
            if cond_expr and isinstance(cond_expr, str):
                issues.extend(self._validate_condition(cond_expr))

        # --- handler ---
        handler = data.get("handler")
        if handler is None:
            issues.append(Issue("error", "Missing 'handler' — every route needs a handler", "handler"))
        elif isinstance(handler, dict):
            issues.extend(self._validate_handler(handler, "handler"))
        elif isinstance(handler, str):
            pass  # reference to heap object — valid

        # --- heap ---
        heap = data.get("heap")
        if heap is not None:
            if not isinstance(heap, list):
                issues.append(Issue("error", "'heap' must be an array", "heap"))
            else:
                issues.extend(self._validate_heap(heap))

        # --- secrets ---
        issues.extend(self._check_secrets(content))

        # --- session ---
        if "session" in data:
            session = data["session"]
            if isinstance(session, dict):
                if "type" not in session:
                    issues.append(Issue("warning",
                        "Session object missing 'type' — defaults to JwtSession", "session"))

        return issues

    def _validate_base_uri(self, uri: str) -> list[Issue]:
        issues = []
        parsed = urlparse(uri)

        if not parsed.scheme:
            issues.append(Issue("error",
                f"baseURI '{uri}' missing scheme — must start with http:// or https://",
                "baseURI"))
            return issues

        if parsed.scheme not in ("http", "https"):
            issues.append(Issue("error",
                f"baseURI scheme '{parsed.scheme}' is invalid — use http or https",
                "baseURI"))
            return issues

        if parsed.scheme == "http":
            issues.append(Issue("warning",
                f"baseURI uses HTTP (not HTTPS) — consider using HTTPS for security",
                "baseURI"))

        if not parsed.hostname:
            issues.append(Issue("error",
                f"baseURI '{uri}' missing hostname", "baseURI"))
            return issues

        # Check for localhost/127.0.0.1 (likely dev config)
        if parsed.hostname in ("localhost", "127.0.0.1", "0.0.0.0"):
            issues.append(Issue("warning",
                f"baseURI points to {parsed.hostname} — is this intentional for production?",
                "baseURI"))

        # Check for trailing slash
        if parsed.path and parsed.path != "/" and parsed.path.endswith("/"):
            issues.append(Issue("warning",
                "baseURI has trailing slash — may cause double-slash in proxied URLs",
                "baseURI"))

        # Check port
        if parsed.port:
            if parsed.scheme == "https" and parsed.port == 80:
                issues.append(Issue("warning",
                    "baseURI uses port 80 with HTTPS — did you mean 443?", "baseURI"))
            elif parsed.scheme == "http" and parsed.port == 443:
                issues.append(Issue("warning",
                    "baseURI uses port 443 with HTTP — did you mean HTTPS?", "baseURI"))

        return issues

    def _validate_condition(self, expr: str) -> list[Issue]:
        issues = []
        # Should be a PingGateway expression ${...}
        stripped = expr.strip()
        if not stripped.startswith("${") or not stripped.endswith("}"):
            issues.append(Issue("warning",
                "Condition should be an expression like ${matches(request.uri.path, '^/api')}",
                "condition"))
        else:
            inner = stripped[2:-1].strip()
            if not inner:
                issues.append(Issue("error", "Condition expression is empty", "condition"))
            # Check for common mistakes
            if "request.uri.path" not in inner and "request.uri" not in inner and "find" not in inner.lower():
                issues.append(Issue("warning",
                    "Condition doesn't reference request.uri.path — is this correct?",
                    "condition"))
        return issues

    def _validate_handler(self, handler: dict, path: str) -> list[Issue]:
        issues = []
        handler_type = handler.get("type")
        if not handler_type:
            issues.append(Issue("warning",
                f"Handler at '{path}' missing 'type'", path))
            return issues

        if handler_type not in _KNOWN_HANDLER_TYPES:
            issues.append(Issue("warning",
                f"Unknown handler type '{handler_type}' at '{path}' — "
                f"known types: {', '.join(sorted(_KNOWN_HANDLER_TYPES))}",
                path))

        config = handler.get("config")
        if handler_type == "Chain" and isinstance(config, dict):
            filters = config.get("filters")
            if filters is not None:
                if not isinstance(filters, list):
                    issues.append(Issue("error",
                        f"Chain filters at '{path}' must be an array", path))
                else:
                    for i, f in enumerate(filters):
                        if isinstance(f, dict):
                            issues.extend(self._validate_filter(f, f"{path}.filters[{i}]"))

            chain_handler = config.get("handler")
            if chain_handler is None:
                issues.append(Issue("error",
                    f"Chain at '{path}' missing 'handler' in config", path))
            elif isinstance(chain_handler, dict):
                issues.extend(self._validate_handler(chain_handler, f"{path}.handler"))

        if handler_type == "DispatchHandler" and isinstance(config, dict):
            bindings = config.get("bindings")
            if not bindings:
                issues.append(Issue("error",
                    f"DispatchHandler at '{path}' has no bindings", path))
            elif isinstance(bindings, list):
                for i, b in enumerate(bindings):
                    if isinstance(b, dict) and "handler" not in b:
                        issues.append(Issue("error",
                            f"Binding [{i}] in DispatchHandler at '{path}' missing 'handler'",
                            f"{path}.bindings[{i}]"))

        return issues

    def _validate_filter(self, f: dict, path: str) -> list[Issue]:
        issues = []
        f_type = f.get("type")
        if not f_type:
            issues.append(Issue("warning", f"Filter at '{path}' missing 'type'", path))
        elif f_type not in _KNOWN_FILTER_TYPES:
            issues.append(Issue("warning",
                f"Unknown filter type '{f_type}' at '{path}'", path))
        return issues

    def _validate_heap(self, heap: list) -> list[Issue]:
        issues = []
        names_seen = set()
        for i, obj in enumerate(heap):
            if not isinstance(obj, dict):
                issues.append(Issue("error", f"Heap object [{i}] must be a JSON object", f"heap[{i}]"))
                continue
            name = obj.get("name")
            if not name:
                issues.append(Issue("warning", f"Heap object [{i}] missing 'name'", f"heap[{i}]"))
            elif name in names_seen:
                issues.append(Issue("error",
                    f"Duplicate heap object name '{name}'", f"heap[{i}]"))
            else:
                names_seen.add(name)
            if "type" not in obj:
                issues.append(Issue("warning",
                    f"Heap object [{i}] ('{name or '?'}') missing 'type'", f"heap[{i}]"))
        return issues

    def _check_secrets(self, content: str) -> list[Issue]:
        issues = []
        for pattern, msg in _SECRET_PATTERNS:
            if re.search(pattern, content, re.IGNORECASE):
                issues.append(Issue("warning",
                    f"{msg} — use property expressions or secrets store",
                    "security"))
        return issues


# Auto-register
register(PingGatewayValidator())
