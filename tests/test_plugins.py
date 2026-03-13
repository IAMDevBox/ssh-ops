"""Tests for ssh_ops.plugins — file validation plugin system."""

import json

import pytest
import yaml
from fastapi.testclient import TestClient

from ssh_ops.config import AppConfig, TaskConfig
from ssh_ops.logger import ExecLogger
from ssh_ops.plugins import Issue, FileValidator, validate_file, get_plugins
from ssh_ops.plugins.ping_gateway import PingGatewayValidator
from ssh_ops.plugins.shell_script import ShellScriptValidator
from ssh_ops.plugins.yaml_lint import YamlValidator
from ssh_ops.server import create_app


# ---------------------------------------------------------------------------
# Plugin registry
# ---------------------------------------------------------------------------

class TestPluginRegistry:
    def test_plugins_discovered(self):
        plugins = get_plugins()
        names = [p.name for p in plugins]
        assert "ping-gateway" in names
        assert "shell-script" in names
        assert "yaml" in names

    def test_validate_file_returns_none_for_unknown(self):
        assert validate_file("readme.txt", "hello world") is None

    def test_validate_file_returns_list(self):
        result = validate_file("test.sh", "echo hello\n")
        assert isinstance(result, list)
        assert len(result) >= 1
        assert result[0]["plugin"] == "shell-script"

    def test_multi_plugin_not_triggered_for_single_match(self):
        result = validate_file("deploy.sh", "#!/bin/bash\necho hi\n")
        assert len(result) == 1

    def test_issue_to_dict(self):
        i = Issue("error", "bad thing", "field.name")
        d = i.to_dict()
        assert d == {"level": "error", "message": "bad thing", "field": "field.name"}

    def test_issue_to_dict_no_field(self):
        i = Issue("warning", "minor thing")
        d = i.to_dict()
        assert d == {"level": "warning", "message": "minor thing"}

    def test_base_validator_defaults(self):
        v = FileValidator()
        assert v.can_validate("any.txt", "content") is False
        assert v.validate("any.txt", "content") == []


# ---------------------------------------------------------------------------
# PingGateway validator
# ---------------------------------------------------------------------------

class TestPingGateway:
    """Tests for PingGateway / ForgeRock IG route validator."""

    def _make_route(self, **overrides):
        route = {
            "name": "my-route",
            "baseURI": "https://backend.example.com:8443",
            "condition": "${matches(request.uri.path, '^/api')}",
            "handler": {"type": "Chain", "config": {
                "filters": [],
                "handler": {"type": "ClientHandler"}
            }},
        }
        route.update(overrides)
        return json.dumps(route)

    def test_can_validate_valid_route(self):
        v = PingGatewayValidator()
        assert v.can_validate("route.json", self._make_route()) is True

    def test_can_validate_needs_handler(self):
        v = PingGatewayValidator()
        # No handler — should not match
        data = json.dumps({"baseURI": "https://x.com", "condition": "${true}"})
        assert v.can_validate("route.json", data) is False

    def test_can_validate_needs_base_or_condition(self):
        v = PingGatewayValidator()
        # handler only, no baseURI or condition
        data = json.dumps({"handler": {"type": "Chain"}})
        assert v.can_validate("route.json", data) is False

    def test_can_validate_handler_plus_condition(self):
        v = PingGatewayValidator()
        data = json.dumps({"handler": {"type": "Chain"}, "condition": "${true}"})
        assert v.can_validate("route.json", data) is True

    def test_can_validate_handler_plus_base_uri(self):
        v = PingGatewayValidator()
        data = json.dumps({"handler": {"type": "Chain"}, "baseURI": "https://x.com"})
        assert v.can_validate("route.json", data) is True

    def test_can_validate_rejects_non_json(self):
        v = PingGatewayValidator()
        assert v.can_validate("route.json", "not json{") is False

    def test_can_validate_rejects_non_object(self):
        v = PingGatewayValidator()
        assert v.can_validate("route.json", "[1,2,3]") is False

    def test_can_validate_rejects_non_json_extension(self):
        v = PingGatewayValidator()
        assert v.can_validate("route.yaml", self._make_route()) is False

    def test_valid_route_no_errors(self):
        v = PingGatewayValidator()
        issues = v.validate("route.json", self._make_route())
        errors = [i for i in issues if i.level == "error"]
        assert errors == []

    def test_missing_name_warning(self):
        v = PingGatewayValidator()
        content = self._make_route()
        data = json.loads(content)
        del data["name"]
        issues = v.validate("r.json", json.dumps(data))
        msgs = [i.message for i in issues if i.field == "name"]
        assert any("Missing 'name'" in m for m in msgs)

    def test_invalid_name_chars(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(name="bad route!"))
        assert any("unusual characters" in i.message for i in issues)

    def test_base_uri_http_warning(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(baseURI="http://backend.example.com"))
        assert any("HTTP" in i.message and "baseURI" == i.field for i in issues)

    def test_base_uri_missing_scheme(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(baseURI="backend.example.com"))
        errors = [i for i in issues if i.level == "error" and i.field == "baseURI"]
        assert any("missing scheme" in i.message for i in errors)

    def test_base_uri_localhost_warning(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(baseURI="https://localhost:8080"))
        assert any("localhost" in i.message for i in issues)

    def test_base_uri_trailing_slash(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(baseURI="https://backend.example.com/api/"))
        assert any("trailing slash" in i.message for i in issues)

    def test_base_uri_port_mismatch_https_80(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(baseURI="https://x.com:80"))
        assert any("port 80" in i.message for i in issues)

    def test_base_uri_port_mismatch_http_443(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(baseURI="http://x.com:443"))
        assert any("port 443" in i.message for i in issues)

    def test_base_uri_expression_skipped(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(baseURI="${system.baseURI}"))
        uri_issues = [i for i in issues if i.field == "baseURI"]
        assert uri_issues == []

    def test_missing_condition_warning(self):
        v = PingGatewayValidator()
        content = self._make_route()
        data = json.loads(content)
        del data["condition"]
        issues = v.validate("r.json", json.dumps(data))
        assert any("No 'condition'" in i.message for i in issues)

    def test_condition_not_expression(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(condition="true"))
        assert any("expression" in i.message for i in issues)

    def test_condition_empty_expression(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(condition="${}"))
        errors = [i for i in issues if i.level == "error"]
        assert any("empty" in i.message for i in errors)

    def test_condition_no_uri_path_warning(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(condition="${true}"))
        assert any("request.uri.path" in i.message for i in issues)

    def test_condition_dict_form(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(
            condition={"condition": "${matches(request.uri.path, '^/x')}"}
        ))
        cond_errors = [i for i in issues if i.level == "error" and i.field == "condition"]
        assert cond_errors == []

    def test_missing_handler_error(self):
        v = PingGatewayValidator()
        data = {"name": "test", "baseURI": "https://x.com",
                "condition": "${matches(request.uri.path, '^/x')}"}
        issues = v.validate("r.json", json.dumps(data))
        assert any("Missing 'handler'" in i.message for i in issues)

    def test_unknown_handler_type(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(
            handler={"type": "MadeUpHandler"}
        ))
        assert any("Unknown handler type" in i.message for i in issues)

    def test_handler_string_ref_ok(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(handler="MyHeapHandler"))
        handler_issues = [i for i in issues if "handler" in i.field]
        assert handler_issues == []

    def test_chain_missing_handler(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(
            handler={"type": "Chain", "config": {"filters": []}}
        ))
        assert any("missing 'handler' in config" in i.message for i in issues)

    def test_chain_filters_not_array(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(
            handler={"type": "Chain", "config": {"filters": "bad", "handler": {"type": "ClientHandler"}}}
        ))
        assert any("must be an array" in i.message for i in issues)

    def test_unknown_filter_type(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(
            handler={"type": "Chain", "config": {
                "filters": [{"type": "MadeUpFilter"}],
                "handler": {"type": "ClientHandler"}
            }}
        ))
        assert any("Unknown filter type" in i.message for i in issues)

    def test_filter_missing_type(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(
            handler={"type": "Chain", "config": {
                "filters": [{"config": {}}],
                "handler": {"type": "ClientHandler"}
            }}
        ))
        assert any("missing 'type'" in i.message for i in issues)

    def test_dispatch_handler_no_bindings(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(
            handler={"type": "DispatchHandler", "config": {"bindings": []}}
        ))
        assert any("no bindings" in i.message for i in issues)

    def test_dispatch_handler_binding_missing_handler(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(
            handler={"type": "DispatchHandler", "config": {
                "bindings": [{"condition": "${true}"}]
            }}
        ))
        assert any("missing 'handler'" in i.message for i in issues)

    def test_heap_duplicate_name(self):
        v = PingGatewayValidator()
        content = self._make_route()
        data = json.loads(content)
        data["heap"] = [
            {"name": "myObj", "type": "ClientHandler"},
            {"name": "myObj", "type": "ClientHandler"},
        ]
        issues = v.validate("r.json", json.dumps(data))
        assert any("Duplicate heap" in i.message for i in issues)

    def test_heap_not_array(self):
        v = PingGatewayValidator()
        content = self._make_route()
        data = json.loads(content)
        data["heap"] = "bad"
        issues = v.validate("r.json", json.dumps(data))
        assert any("must be an array" in i.message for i in issues)

    def test_heap_missing_name(self):
        v = PingGatewayValidator()
        content = self._make_route()
        data = json.loads(content)
        data["heap"] = [{"type": "ClientHandler"}]
        issues = v.validate("r.json", json.dumps(data))
        assert any("missing 'name'" in i.message for i in issues)

    def test_secret_detection(self):
        v = PingGatewayValidator()
        content = self._make_route()
        data = json.loads(content)
        data["handler"] = {"type": "Chain", "config": {
            "filters": [{"type": "HeaderFilter", "config": {
                "password": "SuperSecret123"
            }}],
            "handler": {"type": "ClientHandler"}
        }}
        issues = v.validate("r.json", json.dumps(data))
        assert any("security" == i.field for i in issues)

    def test_session_missing_type(self):
        v = PingGatewayValidator()
        content = self._make_route()
        data = json.loads(content)
        data["session"] = {"properties": {}}
        issues = v.validate("r.json", json.dumps(data))
        assert any("session" in i.field and "type" in i.message for i in issues)

    def test_name_non_string(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(name=123))
        assert any(i.level == "error" and "'name' must be a string" in i.message for i in issues)

    def test_base_uri_non_string(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(baseURI=42))
        assert any(i.level == "error" and "'baseURI' must be a string" in i.message for i in issues)

    def test_base_uri_invalid_scheme(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(baseURI="ftp://backend.example.com"))
        assert any(i.level == "error" and "invalid" in i.message and "ftp" in i.message for i in issues)

    def test_base_uri_missing_hostname(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(baseURI="https://"))
        assert any(i.level == "error" and "missing hostname" in i.message for i in issues)

    def test_handler_missing_type(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", self._make_route(handler={"config": {}}))
        assert any("missing 'type'" in i.message for i in issues)

    def test_heap_item_not_dict(self):
        v = PingGatewayValidator()
        content = self._make_route()
        data = json.loads(content)
        data["heap"] = ["not-a-dict"]
        issues = v.validate("r.json", json.dumps(data))
        assert any("must be a JSON object" in i.message for i in issues)

    def test_heap_item_missing_type(self):
        v = PingGatewayValidator()
        content = self._make_route()
        data = json.loads(content)
        data["heap"] = [{"name": "myObj"}]
        issues = v.validate("r.json", json.dumps(data))
        assert any("missing 'type'" in i.message for i in issues)

    def test_invalid_json_error(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", "not json {")
        assert any(i.level == "error" and "Invalid JSON" in i.message for i in issues)

    def test_non_object_error(self):
        v = PingGatewayValidator()
        issues = v.validate("r.json", "[1,2]")
        assert any(i.level == "error" and "JSON object" in i.message for i in issues)


# ---------------------------------------------------------------------------
# Shell script validator
# ---------------------------------------------------------------------------

class TestShellScript:
    """Tests for shell script validator."""

    def test_can_validate_sh(self):
        v = ShellScriptValidator()
        assert v.can_validate("deploy.sh", "echo hi") is True

    def test_can_validate_bash(self):
        v = ShellScriptValidator()
        assert v.can_validate("run.bash", "echo hi") is True

    def test_can_validate_shebang(self):
        v = ShellScriptValidator()
        assert v.can_validate("myscript", "#!/bin/bash\necho hi") is True

    def test_can_validate_env_shebang(self):
        v = ShellScriptValidator()
        assert v.can_validate("myscript", "#!/usr/bin/env bash\necho hi") is True

    def test_rejects_non_script(self):
        v = ShellScriptValidator()
        assert v.can_validate("data.json", '{"key":"val"}') is False

    def test_rejects_python_shebang(self):
        v = ShellScriptValidator()
        assert v.can_validate("myscript", "#!/usr/bin/python\nprint('hi')") is False

    def test_missing_shebang(self):
        v = ShellScriptValidator()
        issues = v.validate("test.sh", "echo hello\n")
        assert any("shebang" in i.field for i in issues)

    def test_valid_shebang_no_warning(self):
        v = ShellScriptValidator()
        issues = v.validate("test.sh", "#!/bin/bash\nset -e\necho hello\n")
        shebang_issues = [i for i in issues if i.field == "shebang"]
        assert shebang_issues == []

    def test_missing_set_e(self):
        v = ShellScriptValidator()
        issues = v.validate("test.sh", "#!/bin/bash\necho hello\n")
        assert any("set -e" in i.message for i in issues)

    def test_set_euo_pipefail_ok(self):
        v = ShellScriptValidator()
        issues = v.validate("test.sh", "#!/bin/bash\nset -euo pipefail\necho hello\n")
        errexit_issues = [i for i in issues if i.field == "errexit"]
        assert errexit_issues == []

    def test_set_e_in_options(self):
        v = ShellScriptValidator()
        issues = v.validate("test.sh", "#!/bin/bash\nset -xe\necho hello\n")
        errexit_issues = [i for i in issues if i.field == "errexit"]
        assert errexit_issues == []

    def test_unquoted_var_in_rm(self):
        v = ShellScriptValidator()
        issues = v.validate("test.sh", "#!/bin/bash\nset -e\nrm -rf $DIR/stuff\n")
        assert any("unquoted variable" in i.message and "rm" in i.message for i in issues)

    def test_quoted_var_ok(self):
        v = ShellScriptValidator()
        issues = v.validate("test.sh", '#!/bin/bash\nset -e\nrm -rf "$DIR/stuff"\n')
        rm_issues = [i for i in issues if "rm" in i.message]
        assert rm_issues == []

    def test_cd_without_error_handling(self):
        v = ShellScriptValidator()
        issues = v.validate("test.sh", "#!/bin/bash\nset -e\ncd /tmp\n")
        assert any("cd" in i.message and "error handling" in i.message for i in issues)

    def test_cd_with_or_exit_ok(self):
        v = ShellScriptValidator()
        issues = v.validate("test.sh", "#!/bin/bash\nset -e\ncd /tmp || exit 1\n")
        cd_issues = [i for i in issues if "cd" in i.message and "error handling" in i.message]
        assert cd_issues == []

    def test_hardcoded_password(self):
        v = ShellScriptValidator()
        issues = v.validate("test.sh", '#!/bin/bash\nset -e\nPASSWORD="MySecret123"\n')
        assert any("security" == i.field for i in issues)

    def test_curl_credentials(self):
        v = ShellScriptValidator()
        issues = v.validate("test.sh", '#!/bin/bash\nset -e\ncurl -u admin:secret123 https://api.example.com\n')
        assert any("security" == i.field for i in issues)

    def test_comment_lines_skipped(self):
        v = ShellScriptValidator()
        issues = v.validate("test.sh", "#!/bin/bash\nset -e\n# rm -rf $DIR\n# cd /tmp\n")
        line_issues = [i for i in issues if i.field.startswith("line:")]
        assert line_issues == []

    def test_unusual_shebang(self):
        v = ShellScriptValidator()
        issues = v.validate("test.sh", "#!/usr/bin/perl\nprint 'hi';\n")
        assert any("Unusual shebang" in i.message for i in issues)

    def test_set_eu_ok(self):
        v = ShellScriptValidator()
        issues = v.validate("test.sh", "#!/bin/bash\nset -eu\necho hello\n")
        errexit_issues = [i for i in issues if i.field == "errexit"]
        assert errexit_issues == []

    def test_clean_script_minimal_warnings(self):
        v = ShellScriptValidator()
        script = "#!/bin/bash\nset -euo pipefail\necho 'hello'\nexit 0\n"
        issues = v.validate("test.sh", script)
        errors = [i for i in issues if i.level == "error"]
        assert errors == []


# ---------------------------------------------------------------------------
# YAML validator
# ---------------------------------------------------------------------------

class TestYaml:
    """Tests for YAML file validator."""

    def test_can_validate_yml(self):
        v = YamlValidator()
        assert v.can_validate("config.yml", "key: val") is True

    def test_can_validate_yaml(self):
        v = YamlValidator()
        assert v.can_validate("config.yaml", "key: val") is True

    def test_rejects_non_yaml(self):
        v = YamlValidator()
        assert v.can_validate("config.json", "{}") is False

    def test_valid_yaml_no_errors(self):
        v = YamlValidator()
        content = "servers:\n  - name: web1\n    host: 10.0.0.1\n"
        issues = v.validate("config.yml", content)
        errors = [i for i in issues if i.level == "error"]
        assert errors == []

    def test_tab_character_error(self):
        v = YamlValidator()
        content = "servers:\n\t- name: web1\n"
        issues = v.validate("config.yml", content)
        assert any("tab" in i.message.lower() for i in issues)

    def test_syntax_error(self):
        v = YamlValidator()
        content = "key: [\ninvalid\n"
        issues = v.validate("config.yml", content)
        errors = [i for i in issues if i.level == "error"]
        assert any("syntax error" in i.message.lower() or "YAML" in i.message for i in errors)

    def test_empty_yaml_warning(self):
        v = YamlValidator()
        issues = v.validate("config.yml", "")
        assert any("empty" in i.message.lower() for i in issues)

    def test_duplicate_top_level_key(self):
        v = YamlValidator()
        content = "name: foo\nhost: bar\nname: baz\n"
        issues = v.validate("config.yml", content)
        assert any("Duplicate" in i.message and "name" in i.message for i in issues)

    def test_long_line_warning(self):
        v = YamlValidator()
        content = "key: " + "x" * 600 + "\n"
        issues = v.validate("config.yml", content)
        assert any("long line" in i.message for i in issues)

    def test_normal_length_no_warning(self):
        v = YamlValidator()
        content = "key: " + "x" * 50 + "\n"
        issues = v.validate("config.yml", content)
        long_issues = [i for i in issues if "long line" in i.message]
        assert long_issues == []

    def test_tab_in_comment_ignored(self):
        v = YamlValidator()
        content = "# comment with\ttab\nkey: val\n"
        issues = v.validate("config.yml", content)
        tab_issues = [i for i in issues if "tab" in i.message.lower()]
        assert tab_issues == []

    def test_multi_document_yaml(self):
        v = YamlValidator()
        content = "key1: val1\n---\nkey2: val2\n"
        issues = v.validate("config.yml", content)
        errors = [i for i in issues if i.level == "error"]
        assert errors == []


# ---------------------------------------------------------------------------
# API endpoints (/api/validate-file, /api/plugins)
# ---------------------------------------------------------------------------

class TestValidateFileAPI:
    """Tests for /api/validate-file endpoint."""

    @pytest.fixture
    def tmp_config(self, tmp_path):
        data = {
            "servers": [{"host": "10.0.0.1", "name": "web1", "username": "admin", "password": "pass"}],
            "tasks": [{"command": "whoami"}],
        }
        cfg_path = tmp_path / "test.yml"
        cfg_path.write_text(yaml.dump(data, default_flow_style=False), encoding="utf-8")
        return cfg_path

    @pytest.fixture
    def client(self, tmp_config, tmp_path):
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        return TestClient(app)

    def test_validate_no_match(self, client):
        resp = client.post("/api/validate-file", json={"filename": "notes.txt", "content": "hello"})
        assert resp.status_code == 200
        assert resp.json()["result"] is None

    def test_validate_shell_script(self, client):
        resp = client.post("/api/validate-file", json={"filename": "run.sh", "content": "echo hi\n"})
        data = resp.json()
        assert data["result"] is not None
        assert isinstance(data["result"], list)
        assert data["result"][0]["plugin"] == "shell-script"

    def test_validate_yaml(self, client):
        resp = client.post("/api/validate-file", json={"filename": "cfg.yml", "content": "key: val\n"})
        data = resp.json()
        assert data["result"] is not None
        assert data["result"][0]["plugin"] == "yaml"

    def test_validate_missing_filename(self, client):
        resp = client.post("/api/validate-file", json={"content": "hello"})
        assert "error" in resp.json()

    def test_validate_route_json(self, client):
        route = json.dumps({
            "name": "test",
            "handler": {"type": "Chain", "config": {"filters": [], "handler": {"type": "ClientHandler"}}},
            "baseURI": "https://backend.example.com",
            "condition": "${matches(request.uri.path, '^/api')}",
        })
        resp = client.post("/api/validate-file", json={"filename": "route.json", "content": route})
        data = resp.json()
        assert data["result"] is not None
        assert data["result"][0]["plugin"] == "ping-gateway"


class TestPluginsAPI:
    """Tests for /api/plugins endpoint."""

    @pytest.fixture
    def tmp_config(self, tmp_path):
        data = {
            "servers": [{"host": "10.0.0.1", "name": "web1", "username": "admin", "password": "pass"}],
            "tasks": [{"command": "whoami"}],
        }
        cfg_path = tmp_path / "test.yml"
        cfg_path.write_text(yaml.dump(data, default_flow_style=False), encoding="utf-8")
        return cfg_path

    @pytest.fixture
    def client(self, tmp_config, tmp_path):
        TaskConfig._counter.clear()
        config = AppConfig(tmp_config)
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        logger = ExecLogger(log_dir)
        app = create_app(config, logger)
        return TestClient(app)

    def test_list_plugins(self, client):
        resp = client.get("/api/plugins")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        names = [p["name"] for p in data]
        assert "ping-gateway" in names
        assert "shell-script" in names
        assert "yaml" in names

    def test_plugin_has_description(self, client):
        resp = client.get("/api/plugins")
        for p in resp.json():
            assert "description" in p
            assert len(p["description"]) > 0
