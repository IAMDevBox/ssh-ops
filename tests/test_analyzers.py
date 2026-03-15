"""Tests for the log analyzer framework and all domain-specific analyzers."""

import json
import sys
import pytest
import yaml

from ssh_ops.analyzers import (
    Finding, LogAnalyzer, analyze_log, get_analyzers, load_knowledge,
    match_patterns, aggregate_top_errors, register,
    group_stack_traces, extract_root_cause, discover_analyzers,
)
from ssh_ops.analyzers.generic import GenericAnalyzer
from ssh_ops.analyzers.ping_gateway import PingGatewayAnalyzer
from ssh_ops.analyzers.ping_am import PingAMAnalyzer
from ssh_ops.analyzers.ping_ds import PingDSAnalyzer
from ssh_ops.analyzers.ping_idm import PingIDMAnalyzer
from ssh_ops.config import LogSourceConfig


# ── LogSourceConfig ──────────────────────────────────────────

class TestLogSourceConfig:
    def test_basic(self):
        ls = LogSourceConfig({"name": "test", "path": "/var/log/test.log", "type": "generic"})
        assert ls.name == "test"
        assert ls.path == "/var/log/test.log"
        assert ls.type == "generic"

    def test_default_type(self):
        ls = LogSourceConfig({"name": "test", "path": "/var/log/test.log"})
        assert ls.type == "generic"

    def test_missing_name(self):
        with pytest.raises(ValueError, match="missing 'name'"):
            LogSourceConfig({"path": "/var/log/test.log"})

    def test_missing_path(self):
        with pytest.raises(ValueError, match="missing 'path'"):
            LogSourceConfig({"name": "test"})

    def test_relative_path(self):
        with pytest.raises(ValueError, match="must be absolute"):
            LogSourceConfig({"name": "test", "path": "relative/path.log"})

    def test_to_dict(self):
        ls = LogSourceConfig({"name": "x", "path": "/tmp/x.log", "type": "ping-ds"})
        d = ls.to_dict()
        assert d == {"name": "x", "path": "/tmp/x.log", "type": "ping-ds"}


# ── Framework ────────────────────────────────────────────────

class TestFramework:
    def test_finding_dataclass(self):
        f = Finding(level="error", message="test", category="cat", suggestion="fix it")
        assert f.level == "error"
        assert f.count == 1

    def test_load_knowledge_returns_list(self):
        patterns = load_knowledge("generic")
        assert isinstance(patterns, list)
        assert len(patterns) > 0
        assert "match" in patterns[0]

    def test_load_knowledge_missing_file(self):
        result = load_knowledge("nonexistent_xyz")
        assert result == []

    def test_match_patterns_substring(self):
        patterns = [{"match": "Connection refused", "level": "error"}]
        assert len(match_patterns("ERROR: Connection refused to backend", patterns)) == 1
        assert len(match_patterns("Everything is fine", patterns)) == 0

    def test_match_patterns_regex(self):
        import re
        patterns = [{"match": r"SAML.*signature", "regex": True, "level": "error",
                      "_compiled": re.compile(r"SAML.*signature", re.IGNORECASE)}]
        assert len(match_patterns("SAML response signature invalid", patterns)) == 1
        assert len(match_patterns("No issues", patterns)) == 0

    def test_match_patterns_case_insensitive_substring(self):
        patterns = [{"match": "OutOfMemoryError", "level": "error"}]
        assert len(match_patterns("java.lang.outofmemoryerror: Heap space", patterns)) == 1

    def test_aggregate_top_errors(self):
        msgs = [("err A", 1), ("err B", 2), ("err A", 3), ("err A", 5), ("err B", 6), ("err C", 7)]
        top = aggregate_top_errors(msgs, top_n=2)
        assert len(top) == 2
        assert top[0]["pattern"] == "err A"
        assert top[0]["count"] == 3
        assert top[0]["lines"] == [1, 3, 5]
        assert top[1]["pattern"] == "err B"
        assert top[1]["lines"] == [2, 6]

    def test_get_analyzers_not_empty(self):
        analyzers = get_analyzers()
        names = [a.name for a in analyzers]
        assert "generic" in names
        assert "ping-gateway" in names
        assert "ping-am" in names
        assert "ping-ds" in names
        assert "ping-idm" in names

    def test_analyze_log_fallback_to_generic(self):
        result = analyze_log("generic", ["ERROR: something broke", "INFO: ok"])
        assert result["total_lines"] == 2
        assert result["errors"] >= 1

    def test_analyze_log_unknown_type_falls_back(self):
        result = analyze_log("unknown-type-xyz", ["ERROR: test"])
        assert result["errors"] >= 1

    def test_base_class_can_analyze_returns_false(self):
        # L47: base LogAnalyzer.can_analyze returns False
        base = LogAnalyzer()
        assert base.can_analyze("generic", []) is False

    def test_base_class_analyze_returns_empty(self):
        # L63: base LogAnalyzer.analyze returns empty dict structure
        base = LogAnalyzer()
        result = base.analyze(["line1", "line2"])
        assert result["total_lines"] == 2
        assert result["errors"] == 0
        assert result["warnings"] == 0
        assert result["top_errors"] == []
        assert result["findings"] == []

    def test_load_knowledge_malformed_yaml(self, tmp_path, monkeypatch):
        # L84-86: exception path when YAML is malformed
        import ssh_ops.analyzers as analyzers_mod
        bad_yaml_path = tmp_path / "bad.yml"
        bad_yaml_path.write_text("patterns: [invalid: yaml: :\n  - broken", encoding="utf-8")
        original_dir = analyzers_mod._KNOWLEDGE_DIR
        monkeypatch.setattr(analyzers_mod, "_KNOWLEDGE_DIR", tmp_path)
        result = analyzers_mod.load_knowledge("bad")
        monkeypatch.setattr(analyzers_mod, "_KNOWLEDGE_DIR", original_dir)
        assert result == []

    def test_group_stack_traces_caused_by_with_frames(self):
        # L163-168: flush with Caused by + frames, second Caused by resets root_cause
        lines = [
            "java.lang.RuntimeException: outer error",
            "  at com.example.A.foo(A.java:10)",
            "Caused by: java.io.IOException: inner error",
            "  at com.example.B.bar(B.java:20)",
            "Caused by: java.net.SocketException: connection reset",
            "  at com.example.C.baz(C.java:30)",
            "INFO normal line",
        ]
        result = group_stack_traces(lines)
        # Should produce a merged line for the exception and the normal line
        assert len(result) == 2
        merged = result[0]
        # Second Caused by should be the root_cause (last one wins)
        assert "connection reset" in merged
        assert "at com.example.C.baz" in merged

    def test_group_stack_traces_flush_only_frames(self):
        # L170: flush with frames but no Caused by
        lines = [
            "java.lang.NullPointerException: null ref",
            "  at com.example.X.method(X.java:5)",
            "  at com.example.Y.call(Y.java:15)",
            "INFO done",
        ]
        result = group_stack_traces(lines)
        assert len(result) == 2
        merged = result[0]
        assert "NullPointerException" in merged
        assert "at com.example.X.method" in merged
        # No Caused by present
        assert "Caused by" not in merged

    def test_group_stack_traces_flush_root_cause_no_frames(self):
        # L172: flush with root_cause but no at-frames after Caused by
        lines = [
            "java.lang.Exception: outer",
            "Caused by: java.io.IOException: disk full",
            "INFO next line",
        ]
        result = group_stack_traces(lines)
        assert len(result) == 2
        merged = result[0]
        assert "Caused by" in merged
        assert "disk full" in merged

    def test_group_stack_traces_orphan_trace_line(self):
        # L178-182: orphan stack line (matches _TRACE_LINE_RE but current_block is empty)
        lines = [
            "  at com.example.OrphanFrame(Orphan.java:99)",
            "INFO normal",
        ]
        result = group_stack_traces(lines)
        # Orphan line kept as-is, plus normal line
        assert len(result) == 2
        assert "OrphanFrame" in result[0]
        assert "INFO normal" in result[1]

    def test_extract_root_cause_with_caused_by(self):
        # L202-207: extract_root_cause finds the last Caused by segment
        merged = "java.lang.RuntimeException: outer | Caused by: java.io.IOException: disk full | at com.example.A.foo(A.java:10)"
        result = extract_root_cause(merged)
        assert result.startswith("Caused by")
        assert "disk full" in result

    def test_extract_root_cause_no_caused_by(self):
        # L208-209: no Caused by — returns first part
        merged = "java.lang.NullPointerException: null ref | at com.example.X.m(X.java:5)"
        result = extract_root_cause(merged)
        assert result == "java.lang.NullPointerException: null ref"

    def test_analyze_log_specific_analyzer_claims(self):
        # L236: a specific analyzer claims the log type
        lines = [
            '{"timestamp":"2026-03-13 10:00:00","level":"ERROR","thread":"t1","mdc":{},"logger":"org.forgerock.openam.auth","message":"Login failed"}',
        ]
        result = analyze_log("ping-am", lines)
        assert result["errors"] == 1

    def test_analyze_log_no_analyzers_fallback(self, monkeypatch):
        # L242: no analyzers at all registered → returns fallback dict
        import ssh_ops.analyzers as analyzers_mod
        original = analyzers_mod._analyzers[:]
        analyzers_mod._analyzers.clear()
        try:
            result = analyzers_mod.analyze_log("unknown", ["ERROR: test"])
            assert result["summary"] == "No analyzer available"
            assert result["total_lines"] == 1
        finally:
            analyzers_mod._analyzers[:] = original

    def test_discover_analyzers_import_error(self, monkeypatch, caplog):
        # L252, 255-256: import error path in discover_analyzers;
        # also exercises L252 (continue for _-prefixed names)
        import pkgutil
        import logging

        def fake_iter_modules(path):
            yield (None, "_private_module", False)   # should be skipped (L252 continue)
            yield (None, "bad_module_xyz", False)    # should fail import

        monkeypatch.setattr(pkgutil, "iter_modules", fake_iter_modules)
        import importlib
        original_import = importlib.import_module

        def failing_import(name):
            if "bad_module_xyz" in name:
                raise ImportError("simulated import failure")
            return original_import(name)

        monkeypatch.setattr(importlib, "import_module", failing_import)

        with caplog.at_level(logging.WARNING, logger="ssh_ops.analyzers"):
            discover_analyzers()

        assert any("bad_module_xyz" in r.message for r in caplog.records)


# ── Generic Analyzer ─────────────────────────────────────────

class TestGenericAnalyzer:
    def setup_method(self):
        self.analyzer = GenericAnalyzer()

    def test_can_analyze(self):
        assert self.analyzer.can_analyze("generic", [])
        assert self.analyzer.can_analyze("", [])
        assert not self.analyzer.can_analyze("ping-ds", [])

    def test_empty_input(self):
        result = self.analyzer.analyze([])
        assert result["total_lines"] == 0
        assert result["errors"] == 0

    def test_error_counting(self):
        lines = [
            "2026-03-13 10:00:00 ERROR something failed",
            "2026-03-13 10:00:01 INFO all good",
            "2026-03-13 10:00:02 WARN low memory",
            "2026-03-13 10:00:03 FATAL crash",
            "2026-03-13 10:00:04 CRITICAL disk full",
        ]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 3  # ERROR + FATAL + CRITICAL
        assert result["warnings"] == 1  # WARN

    def test_top_errors_aggregation(self):
        lines = [
            "ERROR: Connection refused",
            "ERROR: Connection refused",
            "ERROR: Timeout",
            "ERROR: Connection refused",
        ]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 4
        assert result["top_errors"][0]["count"] == 3

    def test_knowledge_pattern_matching(self):
        lines = ["OutOfMemoryError: Java heap space"]
        result = self.analyzer.analyze(lines)
        assert len(result["findings"]) >= 1
        assert any("memory" in f["message"].lower() for f in result["findings"])

    def test_summary(self):
        lines = ["ERROR: x", "WARN: y"]
        result = self.analyzer.analyze(lines)
        assert "2 lines" in result["summary"]

    def test_blank_lines_skipped(self):
        # L50: blank lines in input are skipped
        lines = ["", "   ", "ERROR: something broke", ""]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 1
        assert result["total_lines"] == 4

    def test_long_error_message_truncated(self):
        # L62: error message > 200 chars gets truncated with "..."
        long_msg = "A" * 250
        lines = [f"ERROR: {long_msg}"]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 1
        top = result["top_errors"]
        assert len(top) == 1
        assert top[0]["pattern"].endswith("...")
        assert len(top[0]["pattern"]) <= 204  # 200 + "..."


# ── PingGateway Analyzer ─────────────────────────────────────

class TestPingGatewayAnalyzer:
    def setup_method(self):
        self.analyzer = PingGatewayAnalyzer()

    def _ig_line(self, level, route, message):
        return f"2026-03-13T10:15:32,456+00:00 | {level} | http-exec-1 | Handler | @{route} | {message}"

    def test_can_analyze_by_type(self):
        assert self.analyzer.can_analyze("ping-gateway", [])

    def test_can_analyze_auto_detect(self):
        lines = [
            self._ig_line("ERROR", "myroute", "test error"),
            self._ig_line("INFO", "myroute", "test info"),
            self._ig_line("WARN", "other", "test warn"),
        ]
        assert self.analyzer.can_analyze("", lines)

    def test_cannot_analyze_unrelated(self):
        assert not self.analyzer.can_analyze("", ["just some text", "nothing special"])

    def test_error_counting(self):
        lines = [
            self._ig_line("ERROR", "route1", "Connection refused"),
            self._ig_line("ERROR", "route1", "Connection refused"),
            self._ig_line("WARN", "route2", "Slow response"),
            self._ig_line("INFO", "route1", "Request processed"),
        ]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 2
        assert result["warnings"] == 1
        assert result["by_route"]["route1"]["errors"] == 2
        assert result["by_route"]["route2"]["warnings"] == 1

    def test_knowledge_findings(self):
        lines = [
            self._ig_line("ERROR", "api", "no handler to dispatch to"),
            self._ig_line("ERROR", "api", "SSLHandshakeException: cert expired"),
        ]
        result = self.analyzer.analyze(lines)
        assert len(result["findings"]) >= 2
        descs = [f["message"] for f in result["findings"]]
        assert any("route" in d.lower() or "dispatch" in d.lower() for d in descs)
        assert any("tls" in d.lower() or "handshake" in d.lower() for d in descs)

    def test_empty_input(self):
        result = self.analyzer.analyze([])
        assert result["total_lines"] == 0

    def test_non_matching_lines_skipped(self):
        lines = ["some random text", "not a log line"]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 0

    def test_summary_contains_route(self):
        lines = [self._ig_line("ERROR", "myroute", "fail")]
        result = self.analyzer.analyze(lines)
        assert "myroute" in result["summary"]

    def test_blank_lines_skipped_in_analyze(self):
        # L62: blank lines skipped during analysis
        lines = ["", "   ", self._ig_line("ERROR", "route1", "failed"), ""]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 1
        assert result["total_lines"] == 4

    def test_warning_increments_warnings_counter(self):
        # L90-91: WARNING match increments warnings counter in by_category when knowledge pattern matches
        # "URI Too Long" is a warning-level knowledge pattern
        lines = [
            self._ig_line("WARN", "route1", "URI Too Long — request rejected"),
            self._ig_line("WARNING", "route2", "HTTP 503 Service Unavailable from backend"),
        ]
        result = self.analyzer.analyze(lines)
        assert result["warnings"] == 2
        assert result["by_route"]["route1"]["warnings"] == 1
        assert result["by_route"]["route2"]["warnings"] == 1
        # by_category should have warning counts from knowledge pattern matches
        assert any(v.get("warnings", 0) > 0 for v in result["by_category"].values())


# ── PingAM Analyzer ──────────────────────────────────────────

class TestPingAMAnalyzer:
    def setup_method(self):
        self.analyzer = PingAMAnalyzer()

    def _json_line(self, level, logger, message):
        return json.dumps({
            "timestamp": "2026-03-13 10:15:32.456",
            "level": level,
            "thread": "http-exec-1",
            "mdc": {"transactionId": "tx-123"},
            "logger": f"org.forgerock.openam.{logger}",
            "message": message,
        })

    def test_can_analyze_by_type(self):
        assert self.analyzer.can_analyze("ping-am", [])

    def test_can_analyze_auto_detect_json(self):
        lines = [self._json_line("ERROR", "authentication", "Login failed")]
        assert self.analyzer.can_analyze("", lines)

    def test_can_analyze_auto_detect_text(self):
        lines = ["amAuthentication:03/13/2026 10:15:32:456 AM EDT: Thread[http-exec-1,5,main]"]
        assert self.analyzer.can_analyze("", lines)

    def test_json_error_counting(self):
        lines = [
            self._json_line("ERROR", "authentication", "Login failed"),
            self._json_line("ERROR", "authentication", "Login failed"),
            self._json_line("WARNING", "session", "Session timeout"),
            self._json_line("INFO", "core", "Startup complete"),
        ]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 2
        assert result["warnings"] == 1
        assert result["by_category"]["authentication"]["errors"] == 2

    def test_json_knowledge_findings(self):
        lines = [
            self._json_line("ERROR", "authentication", "Login failed for user admin"),
            self._json_line("ERROR", "federation", "SAML response signature invalid"),
        ]
        result = self.analyzer.analyze(lines)
        assert len(result["findings"]) >= 2

    def test_text_format_analysis(self):
        lines = [
            "amAuthentication:03/13/2026 10:15:32:456 AM EDT: Thread[http-exec-1,5,main]",
            "ERROR: Login failed for user admin",
            "amSession:03/13/2026 10:15:33:000 AM EDT: Thread[http-exec-2,5,main]",
            "WARNING: Session not found for token xyz",
        ]
        result = self.analyzer.analyze(lines)
        assert result["errors"] >= 1
        assert result["warnings"] >= 1

    def test_empty_input(self):
        result = self.analyzer.analyze([])
        assert result["total_lines"] == 0

    def test_invalid_json_skipped(self):
        lines = ["{invalid json", '{"level":"INFO","message":"ok"}']
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 0

    def test_can_analyze_skips_blank_lines_in_sample(self):
        # L68: blank lines in sample skipped during can_analyze
        lines = ["", "   ", self._json_line("INFO", "authentication", "Startup")]
        # Should still detect as ping-am due to openam logger
        assert self.analyzer.can_analyze("", lines)

    def test_can_analyze_invalid_json_line(self):
        # L75-76: invalid JSON line (JSONDecodeError caught), does not crash
        lines = ["{not valid json at all", self._json_line("ERROR", "core", "boot")]
        # Invalid JSON is skipped; valid line has openam logger → returns True
        assert self.analyzer.can_analyze("", lines)

    def test_can_analyze_invalid_json_only(self):
        # L75-76: only invalid JSON, no legacy either → False
        lines = ["{bad json}", "{also bad}"]
        assert not self.analyzer.can_analyze("", lines)

    def test_json_analysis_non_json_line_skipped(self):
        # L108: non-JSON line (doesn't start with '{') during JSON analysis is skipped
        lines = [
            self._json_line("ERROR", "authentication", "Login failed"),
            "plain text line mixed in",
            self._json_line("INFO", "core", "ok"),
        ]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 1

    def test_json_malformed_line_during_analysis_skipped(self):
        # L111: malformed JSON line (starts with '{' but invalid) is caught and skipped
        lines = [
            self._json_line("ERROR", "authentication", "Login failed"),
            "{this is not valid json}",
            self._json_line("INFO", "core", "ok"),
        ]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 1

    def test_text_format_blank_lines_skipped(self):
        # L160: blank lines in text analysis are skipped (continue)
        lines = [
            "amAuthentication:03/13/2026 10:15:32:456 AM EDT: Thread[http-exec-1,5,main]",
            "",
            "   ",
            "ERROR: Login failed",
        ]
        result = self.analyzer.analyze(lines)
        assert result["errors"] >= 1

    def test_text_format_warning_line(self):
        # L160: WARNING-level line in text format increments warnings
        lines = [
            "amAuthentication:03/13/2026 10:15:32:456 AM EDT: Thread[http-exec-1,5,main]",
            "WARNING: Session cookie is expiring soon",
        ]
        result = self.analyzer.analyze(lines)
        assert result["warnings"] >= 1


# ── PingDS Analyzer ──────────────────────────────────────────

class TestPingDSAnalyzer:
    def setup_method(self):
        self.analyzer = PingDSAnalyzer()

    def _ds_line(self, category, severity, msgid, message):
        return f"[13/Mar/2026:10:15:32.456 +0000] category={category} severity={severity} msgID={msgid} msg={message}"

    def test_can_analyze_by_type(self):
        assert self.analyzer.can_analyze("ping-ds", [])

    def test_can_analyze_auto_detect(self):
        lines = [
            self._ds_line("CORE", "ERROR", "123", "test error"),
            self._ds_line("CORE", "INFO", "124", "test info"),
        ]
        assert self.analyzer.can_analyze("", lines)

    def test_cannot_analyze_unrelated(self):
        assert not self.analyzer.can_analyze("", ["just text"])

    def test_error_counting(self):
        lines = [
            self._ds_line("CORE", "ERROR", "100", "Disk space low on /data"),
            self._ds_line("SYNC", "WARNING", "200", "Replication lag detected"),
            self._ds_line("BACKEND", "ERROR", "300", "Index corrupt"),
            self._ds_line("CORE", "NOTICE", "400", "Server started"),
        ]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 2
        assert result["warnings"] == 1
        assert result["by_category"]["CORE"]["errors"] == 1
        assert result["by_category"]["BACKEND"]["errors"] == 1

    def test_knowledge_findings(self):
        lines = [
            self._ds_line("CORE", "ERROR", "100", "Disk space low on /data (95% used)"),
            self._ds_line("SYNC", "ERROR", "200", "SSL handshake failed with peer"),
        ]
        result = self.analyzer.analyze(lines)
        assert len(result["findings"]) >= 2
        suggestions = [f["suggestion"] for f in result["findings"]]
        assert any("disk" in s.lower() for s in suggestions)

    def test_category_labels(self):
        lines = [self._ds_line("CORE", "ERROR", "100", "test")]
        result = self.analyzer.analyze(lines)
        assert result["by_category"]["CORE"]["label"] == "Core Server"

    def test_msgid_in_top_errors(self):
        lines = [self._ds_line("CORE", "ERROR", "8389734", "Something broke")]
        result = self.analyzer.analyze(lines)
        assert "msgID=8389734" in result["top_errors"][0]["pattern"]

    def test_empty_input(self):
        result = self.analyzer.analyze([])
        assert result["total_lines"] == 0

    def test_non_matching_lines_skipped(self):
        lines = ["random text without DS format"]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 0

    def test_non_matching_line_mixed_with_valid(self):
        # L84-85: non-matching line skipped (continues to next line)
        lines = [
            "this does not match the DS format at all",
            self._ds_line("CORE", "ERROR", "100", "Disk space low"),
        ]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 1

    def test_blank_lines_skipped_in_analyze(self):
        # L81: blank lines skipped in analyze loop
        lines = [
            "",
            "   ",
            self._ds_line("CORE", "ERROR", "100", "Disk space low"),
        ]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 1


# ── PingIDM Analyzer ─────────────────────────────────────────

class TestPingIDMAnalyzer:
    def setup_method(self):
        self.analyzer = PingIDMAnalyzer()

    def _json_line(self, level, logger, message, context=""):
        return json.dumps({
            "timestamp": "2026-03-13T10:15:32.456Z",
            "level": level,
            "threadName": "pool-5-thread-3",
            "loggerName": f"org.forgerock.openidm.{logger}",
            "context": context,
            "mdc": {"transactionId": "tx-456"},
            "formattedMessage": message,
        })

    def test_can_analyze_by_type(self):
        assert self.analyzer.can_analyze("ping-idm", [])

    def test_can_analyze_auto_detect_json(self):
        lines = [self._json_line("ERROR", "sync.impl.ReconciliationService", "Recon failed")]
        assert self.analyzer.can_analyze("", lines)

    def test_json_error_counting(self):
        lines = [
            self._json_line("ERROR", "sync.impl.ReconciliationService", "Reconciliation failed"),
            self._json_line("ERROR", "connector.ldap", "LdapException: connection refused"),
            self._json_line("WARNING", "script.javascript", "Script returned null"),
            self._json_line("INFO", "servlet.internal", "Request handled"),
        ]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 2
        assert result["warnings"] == 1
        assert result["by_category"]["sync"]["errors"] == 1
        assert result["by_category"]["connector"]["errors"] == 1

    def test_json_knowledge_findings(self):
        lines = [
            self._json_line("ERROR", "sync", "Reconciliation failed for mapping system_managed"),
            self._json_line("ERROR", "script", "ScriptException in onCreate.js"),
        ]
        result = self.analyzer.analyze(lines)
        assert len(result["findings"]) >= 2

    def test_text_format_analysis(self):
        lines = [
            "[19] Mar 13, 2026 10:15:32.456 AM org.forgerock.openidm.sync.impl.ReconciliationService reconcile",
            "SEVERE: Reconciliation failed for mapping systemLdap_managedUser",
            "[20] Mar 13, 2026 10:15:33.000 AM org.forgerock.openidm.script.javascript execute",
            "WARNING: Script returned null",
        ]
        result = self.analyzer.analyze(lines)
        assert result["errors"] >= 1
        assert result["warnings"] >= 1

    def test_empty_input(self):
        result = self.analyzer.analyze([])
        assert result["total_lines"] == 0

    def test_invalid_json_skipped(self):
        lines = ["{not valid json}", "plain text"]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 0

    def test_summary_components(self):
        lines = [
            self._json_line("ERROR", "sync", "Reconciliation failed"),
        ]
        result = self.analyzer.analyze(lines)
        assert "sync" in result["summary"]

    def test_classify_logger_unknown_category(self):
        # L60: logger name not matching any category → "other"
        from ssh_ops.analyzers.ping_idm import _classify_logger
        result = _classify_logger("org.forgerock.openidm.completely.unknown.logger")
        assert result == "other"

    def test_can_analyze_skips_blank_lines_in_sample(self):
        # L77: blank lines in sample are skipped during can_analyze
        lines = ["", "   ", self._json_line("INFO", "sync.impl.ReconciliationService", "ok")]
        assert self.analyzer.can_analyze("", lines)

    def test_can_analyze_invalid_json_in_sample(self):
        # L84-85: invalid JSON in sample is caught, doesn't crash
        lines = ["{bad json}", self._json_line("ERROR", "sync", "Recon failed")]
        assert self.analyzer.can_analyze("", lines)

    def test_can_analyze_invalid_json_only(self):
        # L84-85: only invalid JSON, no match → False
        lines = ["{bad json}", "{another bad line}"]
        assert not self.analyzer.can_analyze("", lines)

    def test_can_analyze_legacy_format_detection(self):
        # L88: legacy format detection (line contains "openidm" and matches _LEGACY_HEADER_RE)
        legacy_header = "[19] Mar 13, 2026 10:15:32.456 AM org.forgerock.openidm.sync.impl.ReconciliationService reconcile"
        assert self.analyzer.can_analyze("", [legacy_header])

    def test_text_format_blank_lines_skipped(self):
        # L169: blank lines in text analysis are skipped
        lines = [
            "[19] Mar 13, 2026 10:15:32.456 AM org.forgerock.openidm.sync.impl.ReconciliationService reconcile",
            "",
            "   ",
            "SEVERE: Reconciliation failed",
        ]
        result = self.analyzer.analyze(lines)
        assert result["errors"] == 1

    def test_text_format_line_not_matching_level_re(self):
        # L184-185: line not matching _LEVEL_LINE_RE → message = stripped, level = ""
        lines = [
            "[19] Mar 13, 2026 10:15:32.456 AM org.forgerock.openidm.sync.impl.ReconciliationService reconcile",
            "This is a plain message with no level prefix",
        ]
        result = self.analyzer.analyze(lines)
        # Plain message has no level, should not count as error or warning
        assert result["errors"] == 0
        assert result["warnings"] == 0


# ── API Endpoints ────────────────────────────────────────────

class TestLogAnalyzerAPI:
    """Test log analyzer API endpoints using FastAPI test client."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        config_content = {
            "servers": [{"host": "test-node", "name": "test-node"}],
            "tasks": [],
            "logs": [
                {"name": "test-log", "path": "/var/log/test.log", "type": "generic"},
                {"name": "ig-log", "path": "/opt/ig/logs/route.log", "type": "ping-gateway"},
            ],
        }
        self.config_file = tmp_path / "test.yml"
        import yaml as _yaml
        self.config_file.write_text(_yaml.dump(config_content))

        from ssh_ops.config import AppConfig
        from ssh_ops.server import create_app
        from ssh_ops.logger import ExecLogger

        self.config = AppConfig(self.config_file)
        self.logger = ExecLogger(tmp_path / "logs")
        self.app = create_app(self.config, self.logger)

        from starlette.testclient import TestClient
        self.client = TestClient(self.app)

    def test_get_log_sources(self):
        resp = self.client.get("/api/log-sources")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["name"] == "test-log"
        assert data[1]["type"] == "ping-gateway"

    def test_get_analyzers(self):
        resp = self.client.get("/api/analyzers")
        assert resp.status_code == 200
        data = resp.json()
        names = [a["name"] for a in data]
        assert "generic" in names
        assert "ping-gateway" in names

    def test_add_log_source(self):
        resp = self.client.post("/api/add-log-source", json={
            "name": "new-log", "path": "/var/log/new.log", "type": "ping-ds"
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        # Verify it was persisted
        resp2 = self.client.get("/api/log-sources")
        assert len(resp2.json()) == 3

    def test_add_log_source_duplicate(self):
        resp = self.client.post("/api/add-log-source", json={
            "name": "test-log", "path": "/var/log/dup.log"
        })
        data = resp.json()
        assert "already exists" in data["error"]

    def test_add_log_source_validation(self):
        resp = self.client.post("/api/add-log-source", json={"name": "", "path": "/x"})
        assert "required" in resp.json()["error"]

        resp = self.client.post("/api/add-log-source", json={"name": "x", "path": "relative"})
        assert "absolute" in resp.json()["error"]

    def test_delete_log_source(self):
        resp = self.client.post("/api/delete-log-source", json={"index": 0})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["removed"]["name"] == "test-log"
        # Verify
        resp2 = self.client.get("/api/log-sources")
        assert len(resp2.json()) == 1

    def test_delete_log_source_out_of_range(self):
        resp = self.client.post("/api/delete-log-source", json={"index": 99})
        assert "out of range" in resp.json()["error"]

    def test_analyze_logs_no_path(self):
        resp = self.client.post("/api/analyze-logs", json={
            "servers": ["test-node"], "log_path": ""
        })
        assert "required" in resp.json()["error"]

    def test_analyze_logs_no_servers(self):
        resp = self.client.post("/api/analyze-logs", json={
            "servers": ["nonexistent"], "log_path": "/var/log/test.log"
        })
        assert "no servers" in resp.json()["error"]


# ── _build_log_command ───────────────────────────────────────

class TestBuildLogCommand:
    def test_tail_mode(self):
        from ssh_ops.server import _build_log_command
        cmd = _build_log_command("/var/log/test.log", "tail", 1000, "", "", "")
        assert "tail -n 1000" in cmd
        assert "/var/log/test.log" in cmd

    def test_time_range_mode(self):
        from ssh_ops.server import _build_log_command
        cmd = _build_log_command("/var/log/test.log", "time_range", 2000,
                                  "2026-03-13 08:00", "2026-03-13 09:00", "")
        assert "awk" in cmd
        assert "2026-03-13 08:00" in cmd
        assert "2026-03-13 09:00" in cmd

    def test_time_range_no_end(self):
        from ssh_ops.server import _build_log_command
        cmd = _build_log_command("/var/log/test.log", "time_range", 2000,
                                  "2026-03-13 08:00", "", "")
        assert "awk" in cmd
        assert ",0'" in cmd  # awk range to end of file

    def test_with_keywords(self):
        from ssh_ops.server import _build_log_command
        cmd = _build_log_command("/var/log/test.log", "tail", 500, "", "", "ERROR|Exception")
        assert "grep -E" in cmd
        assert "ERROR|Exception" in cmd

    def test_lines_clamped(self):
        from ssh_ops.server import _build_log_command
        cmd = _build_log_command("/var/log/test.log", "tail", -5, "", "", "")
        assert "tail -n 1" in cmd  # clamped to min 1

        cmd2 = _build_log_command("/var/log/test.log", "tail", 999999, "", "", "")
        assert "tail -n 100000" in cmd2  # clamped to max
