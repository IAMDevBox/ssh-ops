"""Tests for ssh_ops.config — parsing, validation, env var resolution."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import yaml

from ssh_ops.yaml_util import yaml_transaction
from ssh_ops.config import (
    AppConfig,
    AutoBackupConfig,
    LogSourceConfig,
    ProxyConfig,
    ServerConfig,
    TaskConfig,
    check_interactive_command,
    is_modifying_command,
    wrap_backup_command,
    _deep_merge,
    _load_global_config,
    _parse_server,
    _resolve_env_vars,
    _auto_detect_key,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_config(tmp_path: Path, data: dict) -> Path:
    """Write a YAML config file and return its path."""
    cfg = tmp_path / "test.yml"
    cfg.write_text(yaml.dump(data, default_flow_style=False), encoding="utf-8")
    return cfg


def _make_task(overrides: dict | None = None) -> TaskConfig:
    """Create a TaskConfig with sensible defaults, overridden by `overrides`."""
    TaskConfig._counter.clear()
    base = {"command": "echo hello"}
    if overrides:
        base.update(overrides)
    return TaskConfig(base)


# ---------------------------------------------------------------------------
# _resolve_env_vars
# ---------------------------------------------------------------------------

class TestResolveEnvVars:
    def test_dollar_var(self, monkeypatch):
        monkeypatch.setenv("MY_PASS", "secret123")
        assert _resolve_env_vars("$MY_PASS") == "secret123"

    def test_braced_var(self, monkeypatch):
        monkeypatch.setenv("MY_PASS", "secret123")
        assert _resolve_env_vars("${MY_PASS}") == "secret123"

    def test_unset_var_kept(self):
        assert _resolve_env_vars("$UNLIKELY_VAR_XYZ") == "$UNLIKELY_VAR_XYZ"

    def test_non_string_passthrough(self):
        assert _resolve_env_vars(42) == 42
        assert _resolve_env_vars(None) is None

    def test_mixed_text(self, monkeypatch):
        monkeypatch.setenv("HOST", "10.0.0.1")
        assert _resolve_env_vars("ssh://$HOST:22") == "ssh://10.0.0.1:22"


# ---------------------------------------------------------------------------
# _parse_server
# ---------------------------------------------------------------------------

class TestParseServer:
    def test_dict_passthrough(self):
        d = {"host": "10.0.0.1", "port": 22, "username": "admin"}
        assert _parse_server(d) is d

    def test_shorthand_user_host(self):
        result = _parse_server("admin@10.0.0.1")
        assert result["host"] == "10.0.0.1"
        assert result["username"] == "admin"
        assert result["port"] == 22

    def test_shorthand_user_host_port(self):
        result = _parse_server("root@192.168.1.1:2222")
        assert result["host"] == "192.168.1.1"
        assert result["username"] == "root"
        assert result["port"] == 2222

    def test_shorthand_host_only(self):
        result = _parse_server("10.0.0.1")
        assert result["host"] == "10.0.0.1"
        assert "username" not in result

    def test_shorthand_invalid_port(self):
        with pytest.raises(ValueError, match="not a number"):
            _parse_server("admin@10.0.0.1:abc")


# ---------------------------------------------------------------------------
# ServerConfig
# ---------------------------------------------------------------------------

class TestServerConfig:
    def test_minimal(self):
        s = ServerConfig({"host": "10.0.0.1"})
        assert s.host == "10.0.0.1"
        assert s.port == 22
        assert s.groups == []

    def test_full(self):
        s = ServerConfig({
            "host": "10.0.0.1",
            "name": "web1",
            "port": 2222,
            "username": "deploy",
            "password": "pass",
            "groups": ["web", "prod"],
        })
        assert s.name == "web1"
        assert s.port == 2222
        assert s.username == "deploy"
        assert s.password == "pass"
        assert s.groups == ["web", "prod"]

    def test_name_defaults_to_host(self):
        s = ServerConfig({"host": "myhost"})
        assert s.name == "myhost"


# ---------------------------------------------------------------------------
# TaskConfig — type detection
# ---------------------------------------------------------------------------

class TestTaskTypeDetection:
    def setup_method(self):
        TaskConfig._counter.clear()

    def test_command_detected(self):
        t = TaskConfig({"command": "whoami"})
        assert t.type == "command"

    def test_upload_detected(self):
        t = TaskConfig({"src": "/opt/a.txt", "dest": "/opt/b.txt"})
        assert t.type == "upload"

    def test_explicit_type(self):
        t = TaskConfig({"type": "script", "src": "/opt/deploy.sh"})
        assert t.type == "script"

    def test_auto_name(self):
        t1 = TaskConfig({"command": "echo 1"})
        t2 = TaskConfig({"command": "echo 2"})
        assert t1.name == "command-1"
        assert t2.name == "command-2"

    def test_custom_name(self):
        t = TaskConfig({"name": "my-task", "command": "ls"})
        assert t.name == "my-task"


# ---------------------------------------------------------------------------
# TaskConfig — validation errors
# ---------------------------------------------------------------------------

class TestTaskValidation:
    def setup_method(self):
        TaskConfig._counter.clear()

    def test_unknown_type(self):
        with pytest.raises(ValueError, match="unknown type"):
            TaskConfig({"type": "bogus", "command": "echo hi"})

    def test_command_empty(self):
        with pytest.raises(ValueError, match="command is required"):
            TaskConfig({"type": "command", "command": ""})

    def test_command_whitespace_only(self):
        with pytest.raises(ValueError, match="command is required"):
            TaskConfig({"type": "command", "command": "   "})

    def test_upload_missing_src(self):
        with pytest.raises(ValueError, match="src is required"):
            TaskConfig({"type": "upload", "dest": "/opt/b.txt"})

    def test_upload_missing_dest(self):
        with pytest.raises(ValueError, match="dest is required"):
            TaskConfig({"type": "upload", "src": "/opt/a.txt"})

    def test_script_missing_src(self):
        with pytest.raises(ValueError, match="src is required"):
            TaskConfig({"type": "script"})

    # --- absolute path validation ---

    def test_src_relative_path_rejected(self):
        with pytest.raises(ValueError, match="src must be an absolute path"):
            TaskConfig({"src": "relative/file.txt", "dest": "/opt/file.txt"})

    def test_src_directory_rejected(self):
        with pytest.raises(ValueError, match="src must be a file path, not a directory"):
            TaskConfig({"src": "/opt/files/", "dest": "/opt/file.txt"})

    def test_dest_relative_rejected(self):
        with pytest.raises(ValueError, match="dest must be an absolute path"):
            TaskConfig({"src": "/opt/a.txt", "dest": "relative/b.txt"})

    def test_dest_directory_rejected(self):
        with pytest.raises(ValueError, match="dest must be a file path, not a directory"):
            TaskConfig({"src": "/opt/a.txt", "dest": "/opt/dir/"})

    def test_src_absolute_linux(self):
        t = TaskConfig({"src": "/opt/file.txt", "dest": "/opt/dest.txt"})
        assert t.src == "/opt/file.txt"

    def test_src_absolute_windows(self):
        t = TaskConfig({"src": "C:\\Users\\file.txt", "dest": "/opt/dest.txt"})
        assert t.src == "C:\\Users\\file.txt"

    # --- timeout ---

    def test_timeout_default(self):
        t = _make_task()
        assert t.timeout == 120

    def test_timeout_custom(self):
        t = _make_task({"timeout": 300})
        assert t.timeout == 300

    def test_timeout_zero_rejected(self):
        with pytest.raises(ValueError, match="timeout must be a positive integer"):
            _make_task({"timeout": 0})

    def test_timeout_negative_rejected(self):
        with pytest.raises(ValueError, match="timeout must be a positive integer"):
            _make_task({"timeout": -1})

    def test_timeout_string_rejected(self):
        with pytest.raises(ValueError, match="timeout must be a positive integer"):
            _make_task({"timeout": "abc"})

    # --- mode ---

    def test_mode_valid_int(self):
        t = _make_task({"mode": 0o755})
        assert t.mode == 0o755

    def test_mode_valid_string(self):
        t = _make_task({"mode": "0755"})
        assert t.mode == "0755"

    def test_mode_invalid_string(self):
        with pytest.raises(ValueError, match="mode must be a valid octal"):
            _make_task({"mode": "999"})

    def test_mode_out_of_range(self):
        with pytest.raises(ValueError, match="mode must be a valid octal"):
            _make_task({"mode": 0o77777})

    def test_mode_none_ok(self):
        t = _make_task()
        assert t.mode is None

    # --- interactive command ---

    def test_interactive_command_rejected(self):
        with pytest.raises(ValueError, match="interactive command"):
            _make_task({"command": "vim /etc/config"})

    def test_tail_f_rejected(self):
        with pytest.raises(ValueError, match="interactive"):
            _make_task({"command": "tail -f /var/log/syslog"})

    def test_ping_without_c_rejected(self):
        with pytest.raises(ValueError, match="ping without -c"):
            _make_task({"command": "ping 10.0.0.1"})

    def test_ping_with_c_ok(self):
        t = _make_task({"command": "ping -c 3 10.0.0.1"})
        assert t.command == "ping -c 3 10.0.0.1"

    # --- _desc for error messages ---

    def test_desc_uses_command(self):
        with pytest.raises(ValueError, match="vim /etc/config"):
            _make_task({"command": "vim /etc/config"})

    def test_desc_uses_src_dest(self):
        with pytest.raises(ValueError, match=r"/bad/src/ -> /opt/dest"):
            TaskConfig({"src": "/bad/src/", "dest": "/opt/dest"})


# ---------------------------------------------------------------------------
# check_interactive_command
# ---------------------------------------------------------------------------

class TestCheckInteractiveCommand:
    def test_empty_ok(self):
        assert check_interactive_command("") is None

    def test_normal_command_ok(self):
        assert check_interactive_command("ls -la") is None
        assert check_interactive_command("df -h") is None

    def test_interactive_blocked(self):
        assert check_interactive_command("top") is not None
        assert check_interactive_command("vim file.txt") is not None
        assert check_interactive_command("less /var/log/syslog") is not None
        assert check_interactive_command("ssh user@host") is not None

    def test_tail_f_blocked(self):
        assert check_interactive_command("tail -f /var/log/syslog") is not None

    def test_tail_n_ok(self):
        assert check_interactive_command("tail -n 20 /var/log/syslog") is None

    def test_ping_no_c_blocked(self):
        assert check_interactive_command("ping 10.0.0.1") is not None

    def test_ping_with_c_ok(self):
        assert check_interactive_command("ping -c 5 10.0.0.1") is None

    def test_watch_blocked(self):
        assert check_interactive_command("watch df -h") is not None

    def test_path_prefix_stripped(self):
        assert check_interactive_command("/usr/bin/vim file.txt") is not None

    # --- Bug 16: pipe/chain detection ---

    def test_pipe_to_interactive_blocked(self):
        assert check_interactive_command("ls | less") is not None

    def test_chain_with_interactive_blocked(self):
        assert check_interactive_command("echo ok && vim file") is not None

    def test_or_chain_with_interactive_blocked(self):
        assert check_interactive_command("false || top") is not None

    def test_semicolon_with_interactive_blocked(self):
        assert check_interactive_command("echo hi; ssh user@host") is not None

    def test_pipe_to_grep_ok(self):
        assert check_interactive_command("ls | grep foo") is None

    def test_chain_safe_commands_ok(self):
        assert check_interactive_command("echo 1 && echo 2") is None

    def test_trailing_pipe_ok(self):
        """Empty sub-command after trailing | should not crash."""
        assert check_interactive_command("echo hi |") is None


# ---------------------------------------------------------------------------
# is_modifying_command
# ---------------------------------------------------------------------------

class TestIsModifyingCommand:
    def test_empty(self):
        assert is_modifying_command("") is False

    def test_safe_commands(self):
        assert is_modifying_command("ls -la") is False
        assert is_modifying_command("cat /etc/hosts") is False
        assert is_modifying_command("whoami") is False

    def test_modifying_commands(self):
        assert is_modifying_command("rm -rf /tmp/test") is True
        assert is_modifying_command("chmod 755 /opt/app") is True
        assert is_modifying_command("kill -9 1234") is True
        assert is_modifying_command("reboot") is True

    def test_sudo_modifying(self):
        assert is_modifying_command("sudo rm -rf /tmp") is True
        assert is_modifying_command("sudo yum install pkg") is True

    def test_sudo_safe(self):
        assert is_modifying_command("sudo ls /root") is False

    def test_systemctl_restart(self):
        assert is_modifying_command("systemctl restart nginx") is True

    def test_systemctl_status(self):
        assert is_modifying_command("systemctl status nginx") is False

    def test_redirect_overwrite(self):
        assert is_modifying_command("echo hi > /tmp/file") is True

    def test_redirect_append_also_flagged(self):
        # >> contains a second > followed by space, which matches >[^>]
        # This is acceptable — both > and >> are flagged as modifying
        result = is_modifying_command("echo hi >> /tmp/file")
        assert result is True


# ---------------------------------------------------------------------------
# AppConfig — full config loading
# ---------------------------------------------------------------------------

class TestAppConfig:
    def test_load_minimal(self, tmp_path):
        data = {
            "servers": [{"host": "10.0.0.1"}],
            "tasks": [{"command": "whoami"}],
        }
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        assert len(config.servers) == 1
        assert len(config.tasks) == 1
        assert config.servers[0].host == "10.0.0.1"
        assert config.tasks[0].command == "whoami"

    def test_default_settings(self, tmp_path):
        data = {"servers": [], "tasks": []}
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        assert config.web_host == "127.0.0.1"
        assert config.web_port == 8080
        assert config.default_timeout == 120
        assert config.keep_alive == 60

    def test_custom_settings(self, tmp_path):
        data = {
            "servers": [],
            "tasks": [],
            "settings": {
                "web_host": "0.0.0.0",
                "web_port": 9090,
                "default_timeout": 300,
            },
        }
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        assert config.web_host == "0.0.0.0"
        assert config.web_port == 9090
        assert config.default_timeout == 300

    def test_env_var_in_password(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_SSH_PASS", "mypassword")
        data = {
            "servers": [{"host": "10.0.0.1", "password": "$TEST_SSH_PASS"}],
            "tasks": [],
        }
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        assert config.servers[0].password == "mypassword"

    def test_server_shorthand(self, tmp_path):
        data = {
            "servers": ["admin@10.0.0.1:2222"],
            "tasks": [],
        }
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        assert config.servers[0].host == "10.0.0.1"
        assert config.servers[0].port == 2222
        assert config.servers[0].username == "admin"

    def test_invalid_task_raises(self, tmp_path):
        data = {
            "servers": [],
            "tasks": [{"type": "bogus"}],
        }
        cfg_path = _write_config(tmp_path, data)
        with pytest.raises(ValueError, match="unknown type"):
            AppConfig(cfg_path)

    # --- filter_servers ---

    def test_filter_by_name(self, tmp_path):
        data = {
            "servers": [
                {"host": "10.0.0.1", "name": "web1"},
                {"host": "10.0.0.2", "name": "web2"},
                {"host": "10.0.0.3", "name": "db1"},
            ],
            "tasks": [],
        }
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        result = config.filter_servers(["web1", "db1"])
        assert len(result) == 2
        assert {s.name for s in result} == {"web1", "db1"}

    def test_filter_by_group(self, tmp_path):
        data = {
            "servers": [
                {"host": "10.0.0.1", "name": "web1", "groups": ["web"]},
                {"host": "10.0.0.2", "name": "web2", "groups": ["web"]},
                {"host": "10.0.0.3", "name": "db1", "groups": ["db"]},
            ],
            "tasks": [],
        }
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        result = config.filter_servers(group="web")
        assert len(result) == 2
        assert all(s.name.startswith("web") for s in result)

    def test_filter_no_filter_returns_all(self, tmp_path):
        data = {
            "servers": [{"host": "10.0.0.1"}, {"host": "10.0.0.2"}],
            "tasks": [],
        }
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        result = config.filter_servers()
        assert len(result) == 2

    # --- get_task ---

    def test_get_task_found(self, tmp_path):
        data = {
            "servers": [],
            "tasks": [{"name": "deploy", "command": "echo deploy"}],
        }
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        t = config.get_task("deploy")
        assert t is not None
        assert t.command == "echo deploy"

    def test_get_task_not_found(self, tmp_path):
        data = {"servers": [], "tasks": []}
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        assert config.get_task("nonexistent") is None

    # --- reload ---

    def test_reload(self, tmp_path):
        data = {"servers": [{"host": "10.0.0.1"}], "tasks": [{"command": "echo 1"}]}
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        assert len(config.tasks) == 1

        # Modify config file
        data["tasks"].append({"command": "echo 2"})
        cfg_path.write_text(yaml.dump(data, default_flow_style=False), encoding="utf-8")
        config.reload()
        assert len(config.tasks) == 2

    # --- list_configs ---

    def test_list_configs(self, tmp_path):
        (tmp_path / "prod.yml").write_text("servers: []")
        (tmp_path / "test.yml").write_text("servers: []")
        (tmp_path / "notes.txt").write_text("not a config")
        configs = AppConfig.list_configs(tmp_path)
        names = [c["name"] for c in configs]
        assert "prod" in names
        assert "test" in names
        assert "notes" not in names

    def test_list_configs_sorted_by_mtime(self, tmp_path):
        import time
        (tmp_path / "old.yml").write_text("servers: []")
        time.sleep(0.05)
        (tmp_path / "new.yml").write_text("servers: []")
        configs = AppConfig.list_configs(tmp_path)
        names = [c["name"] for c in configs]
        assert names.index("new") < names.index("old")
        assert "mtime" in configs[0]

    def test_list_configs_yaml_extension(self, tmp_path):
        (tmp_path / "staging.yaml").write_text("servers: []")
        configs = AppConfig.list_configs(tmp_path)
        names = [c["name"] for c in configs]
        assert "staging" in names

    def test_list_configs_default_dir(self):
        configs = AppConfig.list_configs()
        assert isinstance(configs, list)

    def test_switch_config(self, tmp_path):
        data1 = {"servers": [{"host": "10.0.0.1"}], "tasks": []}
        data2 = {"servers": [{"host": "10.0.0.2"}, {"host": "10.0.0.3"}], "tasks": []}
        (tmp_path / "a").mkdir()
        cfg1 = _write_config(tmp_path / "a", data1)
        cfg2 = tmp_path / "b" / "test2.yml"
        cfg2.parent.mkdir()
        cfg2.write_text(yaml.dump(data2, default_flow_style=False), encoding="utf-8")

        config = AppConfig(cfg1)
        assert len(config.servers) == 1
        config.switch_config(cfg2)
        assert len(config.servers) == 2

    def test_get_server_found(self, tmp_path):
        data = {"servers": [{"host": "10.0.0.1", "name": "web1"}], "tasks": []}
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        s = config.get_server("web1")
        assert s is not None
        assert s.host == "10.0.0.1"

    def test_get_server_not_found(self, tmp_path):
        data = {"servers": [{"host": "10.0.0.1"}], "tasks": []}
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        assert config.get_server("nonexistent") is None


# ---------------------------------------------------------------------------
# __repr__ methods
# ---------------------------------------------------------------------------

class TestReprMethods:
    def setup_method(self):
        TaskConfig._counter.clear()

    def test_server_repr(self):
        s = ServerConfig({"host": "10.0.0.1", "name": "web1", "port": 22})
        r = repr(s)
        assert "web1" in r
        assert "10.0.0.1" in r

    def test_task_repr(self):
        t = TaskConfig({"command": "echo hi"})
        r = repr(t)
        assert "command" in r


# ---------------------------------------------------------------------------
# _auto_detect_key
# ---------------------------------------------------------------------------

class TestAutoDetectKey:
    def test_no_keys(self, monkeypatch, tmp_path):
        monkeypatch.setattr("ssh_ops.config.DEFAULT_KEY_PATHS", [str(tmp_path / "nonexistent")])
        assert _auto_detect_key() is None

    def test_key_found(self, monkeypatch, tmp_path):
        key = tmp_path / "id_rsa"
        key.write_text("fake key")
        monkeypatch.setattr("ssh_ops.config.DEFAULT_KEY_PATHS", [str(key)])
        result = _auto_detect_key()
        assert result is not None
        assert "id_rsa" in result


# ---------------------------------------------------------------------------
# TaskConfig edge cases
# ---------------------------------------------------------------------------

class TestTaskEdgeCases:
    def setup_method(self):
        TaskConfig._counter.clear()

    def test_no_type_no_command_no_src(self):
        """When no type, no command, no src+dest — defaults to command type, then fails validation."""
        with pytest.raises(ValueError, match="command is required"):
            TaskConfig({"name": "empty"})

    def test_mode_string_out_of_range(self):
        with pytest.raises(ValueError, match="mode must be a valid octal"):
            TaskConfig({"command": "echo x", "mode": "77777"})


# ---------------------------------------------------------------------------
# AppConfig._validate_raw — static validation
# ---------------------------------------------------------------------------

class TestValidateRaw:
    def test_valid_config(self):
        AppConfig._validate_raw({"servers": [{"host": "10.0.0.1"}], "tasks": [{"command": "whoami"}]})

    def test_valid_empty(self):
        AppConfig._validate_raw({"servers": [], "tasks": []})

    def test_valid_server_shorthand(self):
        AppConfig._validate_raw({"servers": ["admin@10.0.0.1:22"], "tasks": []})

    def test_not_a_dict(self):
        with pytest.raises(ValueError, match="mapping"):
            AppConfig._validate_raw("not a dict")

    def test_server_not_dict_or_string(self):
        with pytest.raises(ValueError, match="servers\\[0\\]"):
            AppConfig._validate_raw({"servers": [123], "tasks": []})

    def test_server_missing_host(self):
        with pytest.raises(ValueError, match="missing 'host'"):
            AppConfig._validate_raw({"servers": [{"username": "admin"}], "tasks": []})

    def test_task_not_dict(self):
        with pytest.raises(ValueError, match="tasks\\[0\\]"):
            AppConfig._validate_raw({"servers": [], "tasks": ["not a dict"]})

    def test_no_servers_key(self):
        # Should not raise — servers key is optional
        AppConfig._validate_raw({"tasks": []})

    def test_no_tasks_key(self):
        # Should not raise — tasks key is optional
        AppConfig._validate_raw({"servers": []})


# ---------------------------------------------------------------------------
# ProxyConfig
# ---------------------------------------------------------------------------

class TestProxyConfig:
    def test_valid_proxy(self):
        proxy = ProxyConfig({"host": "psmp.corp.com", "user": "jdoe", "account": "admin"})
        assert proxy.type == "psmp"
        assert proxy.host == "psmp.corp.com"
        assert proxy.port == 22
        assert proxy.user == "jdoe"
        assert proxy.account == "admin"
        assert proxy.auth == "otp"

    def test_custom_port_and_auth(self):
        proxy = ProxyConfig({"host": "psmp.corp.com", "user": "jdoe", "account": "admin", "port": 2222, "auth": "password"})
        assert proxy.port == 2222
        assert proxy.auth == "password"

    def test_invalid_type(self):
        with pytest.raises(ValueError, match="Unknown proxy type"):
            ProxyConfig({"type": "bogus", "host": "h", "user": "u", "account": "a"})

    def test_ssh_username(self):
        proxy = ProxyConfig({"host": "psmp.corp.com", "user": "jdoe", "account": "svcadmin"})
        assert proxy.ssh_username("target.corp.com") == "jdoe@svcadmin@target.corp.com"

    def test_repr(self):
        proxy = ProxyConfig({"host": "psmp.corp.com", "user": "jdoe", "account": "admin"})
        r = repr(proxy)
        assert "Proxy(" in r
        assert "jdoe" in r
        assert "psmp.corp.com" in r


# ---------------------------------------------------------------------------
# AppConfig.default_config_path / _save_last_config
# ---------------------------------------------------------------------------

class TestDefaultConfigPath:
    def test_returns_default_when_no_last(self, tmp_path):
        """When no last config file exists, should return DEFAULT_CONFIG."""
        with patch("ssh_ops.config._LAST_CONFIG_FILE", tmp_path / "nonexistent"):
            result = AppConfig.default_config_path()
            assert result.name == "default.yml"

    def test_returns_last_when_exists(self, tmp_path):
        """When last config file points to an existing file, return it."""
        cfg = tmp_path / "myconfig.yml"
        cfg.write_text("servers: []\ntasks: []\n")
        last_file = tmp_path / ".last_config"
        last_file.write_text(str(cfg))
        with patch("ssh_ops.config._LAST_CONFIG_FILE", last_file):
            result = AppConfig.default_config_path()
            assert result == cfg

    def test_returns_default_when_last_path_missing(self, tmp_path):
        """When last config points to nonexistent file, fall back to default."""
        last_file = tmp_path / ".last_config"
        last_file.write_text("/nonexistent/file.yml")
        with patch("ssh_ops.config._LAST_CONFIG_FILE", last_file):
            result = AppConfig.default_config_path()
            assert result.name == "default.yml"


class TestSaveLastConfig:
    def test_save_last_config_oserror(self, tmp_path):
        """OSError during save should be silently ignored."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        with patch("ssh_ops.config._LAST_CONFIG_FILE", tmp_path / "readonly" / "nested" / ".last"), \
             patch("ssh_ops.config._CONFIG_DIR", config_dir):
            # Make parent dir read-only to trigger OSError on mkdir
            readonly_dir = tmp_path / "readonly"
            readonly_dir.mkdir()
            readonly_dir.chmod(0o444)
            try:
                # Should not raise — config is in _CONFIG_DIR but write fails
                AppConfig._save_last_config(config_dir / "test.yml")
            finally:
                readonly_dir.chmod(0o755)

    def test_skips_save_for_non_config_dir(self, tmp_path):
        """Should not save when config file is outside the real config directory."""
        last_file = tmp_path / ".last_config"
        with patch("ssh_ops.config._LAST_CONFIG_FILE", last_file):
            # tmp_path config is not in _CONFIG_DIR, so no save
            AppConfig._save_last_config(tmp_path / "test.yml")
            assert not last_file.exists()


# ---------------------------------------------------------------------------
# wrap_backup_command
# ---------------------------------------------------------------------------

def _cfg(**overrides):
    """Helper to build AutoBackupConfig with defaults."""
    defaults = dict(enabled=True, backup_dir="/opt/backup", commands=["rm", "mv", ">"])
    defaults.update(overrides)
    return AutoBackupConfig(**defaults)


TS = "$(date +%Y%m%d_%H%M%S)"


def _expect(cd="", sudo="", target="", basename="", work_cmd="", backup_dir="/opt/backup"):
    """Build expected backup-wrapped command string."""
    s = "sudo " if sudo else ""
    bak = f"{backup_dir}/{TS}_{basename}"
    return (
        f"{cd}"
        f"if {s}mkdir -p {backup_dir} && {s}cp -a {target} {bak} && echo '[backup] {target} -> {backup_dir}'; then "
        f"{work_cmd}; "
        f"else echo '[WARN] backup failed — command aborted'; exit 1; fi"
    )


class TestWrapBackupCommand:
    """100 % branch coverage for wrap_backup_command."""

    # --- disabled / empty ---------------------------------------------------

    def test_disabled_returns_unchanged(self):
        assert wrap_backup_command("rm /tmp/f", _cfg(enabled=False)) == "rm /tmp/f"

    def test_empty_command_returns_unchanged(self):
        assert wrap_backup_command("", _cfg()) == ""

    def test_whitespace_only_returns_unchanged(self):
        assert wrap_backup_command("   ", _cfg()) == "   "

    # --- rm basic ------------------------------------------------------------

    def test_rm_simple(self):
        result = wrap_backup_command("rm /tmp/f.txt", _cfg())
        assert result == _expect(target="/tmp/f.txt", basename="f.txt", work_cmd="rm /tmp/f.txt")

    def test_rm_with_flags(self):
        result = wrap_backup_command("rm -rf /var/data/", _cfg())
        assert result == _expect(target="/var/data/", basename="data", work_cmd="rm -rf /var/data/")

    def test_rm_multiple_flags(self):
        result = wrap_backup_command("rm -r -f /tmp/dir", _cfg())
        assert result == _expect(target="/tmp/dir", basename="dir", work_cmd="rm -r -f /tmp/dir")

    def test_rm_only_flags_no_target(self):
        assert wrap_backup_command("rm -rf", _cfg()) == "rm -rf"

    # --- mv basic ------------------------------------------------------------

    def test_mv_simple(self):
        result = wrap_backup_command("mv /tmp/a.txt /tmp/b.txt", _cfg())
        assert result == _expect(target="/tmp/a.txt", basename="a.txt", work_cmd="mv /tmp/a.txt /tmp/b.txt")

    def test_mv_with_flags(self):
        result = wrap_backup_command("mv -f /tmp/a /tmp/b", _cfg())
        assert result == _expect(target="/tmp/a", basename="a", work_cmd="mv -f /tmp/a /tmp/b")

    # --- redirect (>) -------------------------------------------------------

    def test_redirect_simple(self):
        result = wrap_backup_command("echo hi > /tmp/out.txt", _cfg())
        assert result == _expect(target="/tmp/out.txt", basename="out.txt", work_cmd="echo hi > /tmp/out.txt")

    def test_redirect_no_slash_in_target(self):
        result = wrap_backup_command("echo x > file.txt", _cfg())
        assert result == _expect(target="file.txt", basename="file.txt", work_cmd="echo x > file.txt")

    def test_redirect_not_in_commands(self):
        """When > is not in commands list, redirect should not trigger backup."""
        cfg = _cfg(commands=["rm", "mv"])
        assert wrap_backup_command("echo hi > /tmp/out.txt", cfg) == "echo hi > /tmp/out.txt"

    # --- cd prefix -----------------------------------------------------------

    def test_cd_prefix_with_rm(self):
        result = wrap_backup_command("cd /home && rm file.sh", _cfg())
        assert result == _expect(cd="cd /home && ", target="file.sh", basename="file.sh", work_cmd="rm file.sh")

    def test_cd_prefix_with_mv(self):
        result = wrap_backup_command("cd /app && mv old.conf new.conf", _cfg())
        assert result == _expect(cd="cd /app && ", target="old.conf", basename="old.conf", work_cmd="mv old.conf new.conf")

    def test_cd_prefix_with_redirect(self):
        result = wrap_backup_command("cd /tmp && echo data > out.log", _cfg())
        assert result == _expect(cd="cd /tmp && ", target="out.log", basename="out.log", work_cmd="echo data > out.log")

    # --- sudo ----------------------------------------------------------------

    def test_sudo_rm(self):
        result = wrap_backup_command("sudo rm /etc/old.conf", _cfg())
        assert result == _expect(sudo=True, target="/etc/old.conf", basename="old.conf", work_cmd="sudo rm /etc/old.conf")

    def test_sudo_mv(self):
        result = wrap_backup_command("sudo mv /etc/a /etc/b", _cfg())
        assert result == _expect(sudo=True, target="/etc/a", basename="a", work_cmd="sudo mv /etc/a /etc/b")

    def test_sudo_redirect(self):
        result = wrap_backup_command("sudo echo x > /etc/conf", _cfg())
        assert result == _expect(sudo=True, target="/etc/conf", basename="conf", work_cmd="sudo echo x > /etc/conf")

    def test_cd_and_sudo_rm(self):
        result = wrap_backup_command("cd /etc && sudo rm old.conf", _cfg())
        assert result == _expect(cd="cd /etc && ", sudo=True, target="old.conf", basename="old.conf", work_cmd="sudo rm old.conf")

    # --- command not in commands list ----------------------------------------

    def test_unmatched_command_unchanged(self):
        assert wrap_backup_command("ls -la /tmp", _cfg()) == "ls -la /tmp"

    def test_cat_unchanged(self):
        assert wrap_backup_command("cat /tmp/file", _cfg()) == "cat /tmp/file"

    # --- unknown command in commands list (else branch) ----------------------

    def test_unknown_cmd_in_commands_list(self):
        """If commands list has 'cp' but wrap_backup only handles rm/mv, returns unchanged."""
        cfg = _cfg(commands=["cp"])
        assert wrap_backup_command("cp /a /b", cfg) == "cp /a /b"

    # --- backup_dir trailing slash -------------------------------------------

    def test_backup_dir_trailing_slash_stripped(self):
        cfg = _cfg(backup_dir="/opt/backup/")
        result = wrap_backup_command("rm /tmp/f", cfg)
        assert "/opt/backup//" not in result
        assert result == _expect(target="/tmp/f", basename="f", work_cmd="rm /tmp/f")

    # --- target with trailing slash ------------------------------------------

    def test_target_trailing_slash_basename(self):
        result = wrap_backup_command("rm -rf /var/log/old/", _cfg())
        assert f"{TS}_old" in result
        assert "[backup]" in result

    # --- empty parts after sudo strip ----------------------------------------

    def test_sudo_only_returns_unchanged(self):
        assert wrap_backup_command("sudo ", _cfg()) == "sudo "

    def test_cd_prefix_sudo_only_returns_unchanged(self):
        """cd prefix + sudo with no actual command → empty parts → unchanged."""
        assert wrap_backup_command("cd /x && sudo ", _cfg()) == "cd /x && sudo "

    # --- mv only flags no target ---------------------------------------------

    def test_mv_only_flags_no_target(self):
        assert wrap_backup_command("mv -f", _cfg()) == "mv -f"

    # --- glob patterns skip backup -------------------------------------------

    def test_rm_star_warns_and_skips(self):
        result = wrap_backup_command("rm *", _cfg())
        assert result.startswith("echo '[WARN] auto-backup skipped")
        assert result.endswith("; rm *")

    def test_rm_prefix_star_warns(self):
        result = wrap_backup_command("rm abc*", _cfg())
        assert "[WARN] auto-backup skipped" in result
        assert "; rm abc*" in result

    def test_rm_question_mark_warns(self):
        result = wrap_backup_command("rm file?.txt", _cfg())
        assert "[WARN] auto-backup skipped" in result

    def test_rm_bracket_warns(self):
        result = wrap_backup_command("rm test[0-9]", _cfg())
        assert "[WARN] auto-backup skipped" in result

    def test_mv_glob_warns(self):
        result = wrap_backup_command("mv *.log /tmp/", _cfg())
        assert "[WARN] auto-backup skipped" in result
        assert "; mv *.log /tmp/" in result

    def test_redirect_glob_warns(self):
        result = wrap_backup_command("echo x > /tmp/out_*", _cfg())
        assert "[WARN] auto-backup skipped" in result

    def test_cd_rm_glob_warns(self):
        result = wrap_backup_command("cd /opt && rm *.bak", _cfg())
        assert "[WARN] auto-backup skipped" in result
        assert "; cd /opt && rm *.bak" in result

    # --- skip backup for files inside backup directory -----------------------

    def test_rm_inside_backup_dir_skips(self):
        assert wrap_backup_command("rm /opt/backup/old_file", _cfg()) == "rm /opt/backup/old_file"

    def test_rm_backup_dir_itself_skips(self):
        assert wrap_backup_command("rm -rf /opt/backup", _cfg()) == "rm -rf /opt/backup"

    def test_rm_backup_dir_trailing_slash_skips(self):
        assert wrap_backup_command("rm -rf /opt/backup/", _cfg()) == "rm -rf /opt/backup/"

    def test_sudo_rm_inside_backup_dir_skips(self):
        assert wrap_backup_command("sudo rm /opt/backup/old", _cfg()) == "sudo rm /opt/backup/old"

    def test_mv_from_backup_dir_skips(self):
        """mv backs up the source; if source is in backup dir, skip."""
        assert wrap_backup_command("mv /opt/backup/f /tmp/", _cfg()) == "mv /opt/backup/f /tmp/"

    def test_redirect_to_backup_dir_skips(self):
        assert wrap_backup_command("echo x > /opt/backup/log", _cfg()) == "echo x > /opt/backup/log"

    def test_rm_outside_backup_dir_still_backed_up(self):
        result = wrap_backup_command("rm /opt/other/file", _cfg())
        assert "cp -a" in result


# ---------------------------------------------------------------------------
# _deep_merge — recursive nested dict merge (L142)
# ---------------------------------------------------------------------------

class TestDeepMerge:
    def test_recursive_nested_merge(self):
        """Both sides have nested dicts under same key — should deep-merge."""
        base = {"a": {"x": 1, "y": 2}, "b": "base_b"}
        override = {"a": {"y": 99, "z": 3}, "c": "override_c"}
        result = _deep_merge(base, override)
        assert result["a"] == {"x": 1, "y": 99, "z": 3}
        assert result["b"] == "base_b"
        assert result["c"] == "override_c"

    def test_non_dict_override_wins(self):
        """When override value is not a dict, it replaces base value."""
        base = {"a": {"x": 1}}
        override = {"a": "scalar"}
        result = _deep_merge(base, override)
        assert result["a"] == "scalar"

    def test_base_non_dict_replaced(self):
        """When base value is not a dict but override is, override wins."""
        base = {"a": "scalar"}
        override = {"a": {"x": 1}}
        result = _deep_merge(base, override)
        assert result["a"] == {"x": 1}


# ---------------------------------------------------------------------------
# _load_global_config — when GLOBAL_CONFIG exists (L150-154)
# ---------------------------------------------------------------------------

class TestLoadGlobalConfig:
    def test_returns_empty_when_file_missing(self, tmp_path):
        """When GLOBAL_CONFIG does not exist, returns {}."""
        with patch("ssh_ops.config.GLOBAL_CONFIG", tmp_path / "nonexistent.yml"):
            # Must call the real function, bypassing conftest patch
            result = _load_global_config()
        assert result == {}

    def test_reads_and_resolves_when_exists(self, tmp_path, monkeypatch):
        """When GLOBAL_CONFIG exists, reads and resolves env vars."""
        monkeypatch.setenv("GLOBAL_HOST", "10.99.99.99")
        global_yml = tmp_path / "global.yml"
        global_yml.write_text("settings:\n  web_host: $GLOBAL_HOST\n", encoding="utf-8")
        with patch("ssh_ops.config.GLOBAL_CONFIG", global_yml):
            result = _load_global_config()
        assert result["settings"]["web_host"] == "10.99.99.99"


# ---------------------------------------------------------------------------
# wrap_backup_command — disabled returns unchanged (L211)
# ---------------------------------------------------------------------------

class TestWrapBackupDisabled:
    def test_disabled_cfg_returns_command_unchanged(self):
        """When cfg.enabled=False, command is returned unchanged (L211 path also via empty-work)."""
        cfg = AutoBackupConfig(enabled=False)
        assert wrap_backup_command("rm /tmp/file", cfg) == "rm /tmp/file"

    def test_cd_prefix_eats_whole_command_returns_unchanged(self):
        """cd prefix extracts entire command leaving empty work_cmd → L211 returns unchanged."""
        # "cd /x && " — regex matches "cd /x && " and work_cmd becomes ""
        result = wrap_backup_command("cd /x && ", _cfg())
        assert result == "cd /x && "

    def test_rm_wildcard_inside_backup_dir_warns(self):
        """rm /opt/backup/* — target is inside backup_dir, has glob → glob warn path (L288)."""
        result = wrap_backup_command("rm /opt/backup/*", _cfg())
        assert "[WARN] auto-backup skipped" in result
        assert "; rm /opt/backup/*" in result

    def test_rm_mixed_glob_and_real_target_emits_glob_warn(self):
        """rm with both real target and glob — backup real target but emit glob warning (L288)."""
        result = wrap_backup_command("rm /tmp/file.txt *.bak", _cfg())
        # Should have a backup command AND a glob warning (not just skip)
        assert "[WARN] auto-backup skipped for wildcard targets" in result
        assert "cp -a /tmp/file.txt" in result


# ---------------------------------------------------------------------------
# ServerConfig — _has_explicit_credentials with key_file (L376)
# ---------------------------------------------------------------------------

class TestServerConfigCredentials:
    def test_has_explicit_credentials_with_key_file(self):
        """_has_explicit_credentials is True when key_file is provided."""
        s = ServerConfig({"host": "10.0.0.1", "key_file": "/home/user/.ssh/id_rsa"})
        assert s._has_explicit_credentials is True
        assert s.key_file == "/home/user/.ssh/id_rsa"

    def test_no_explicit_credentials_without_password_or_key(self):
        """_has_explicit_credentials is False when neither password nor key_file provided."""
        with patch("ssh_ops.config._auto_detect_key", return_value=None):
            s = ServerConfig({"host": "10.0.0.1"})
        assert s._has_explicit_credentials is False

    def test_needs_otp_with_otp_proxy(self):
        """needs_otp returns True when proxy auth is otp (L376)."""
        proxy = ProxyConfig({"host": "psmp.corp.com", "user": "jdoe", "account": "admin", "auth": "otp"})
        s = ServerConfig({"host": "10.0.0.1"}, proxy=proxy)
        assert s.needs_otp is True

    def test_needs_otp_false_without_proxy(self):
        """needs_otp returns False when no proxy."""
        with patch("ssh_ops.config._auto_detect_key", return_value=None):
            s = ServerConfig({"host": "10.0.0.1"})
        assert s.needs_otp is False

    def test_needs_otp_false_with_password_proxy(self):
        """needs_otp returns False when proxy auth is not otp."""
        proxy = ProxyConfig({"host": "psmp.corp.com", "user": "jdoe", "account": "admin", "auth": "password"})
        s = ServerConfig({"host": "10.0.0.1"}, proxy=proxy)
        assert s.needs_otp is False


# ---------------------------------------------------------------------------
# LogSourceConfig — validation and methods (L490-504)
# ---------------------------------------------------------------------------

class TestLogSourceConfig:
    def test_missing_name_raises(self):
        with pytest.raises(ValueError, match="missing 'name'"):
            LogSourceConfig({"path": "/var/log/app.log"})

    def test_missing_path_raises(self):
        with pytest.raises(ValueError, match="missing 'path'"):
            LogSourceConfig({"name": "app"})

    def test_non_absolute_path_raises(self):
        with pytest.raises(ValueError, match="path must be absolute"):
            LogSourceConfig({"name": "app", "path": "relative/log.log"})

    def test_valid_log_source(self):
        ls = LogSourceConfig({"name": "app", "path": "/var/log/app.log", "type": "syslog"})
        assert ls.name == "app"
        assert ls.path == "/var/log/app.log"
        assert ls.type == "syslog"

    def test_to_dict(self):
        ls = LogSourceConfig({"name": "app", "path": "/var/log/app.log"})
        d = ls.to_dict()
        assert d == {"name": "app", "path": "/var/log/app.log", "type": "generic"}

    def test_repr(self):
        ls = LogSourceConfig({"name": "app", "path": "/var/log/app.log"})
        r = repr(ls)
        assert "LogSource" in r
        assert "app" in r
        assert "/var/log/app.log" in r


# ---------------------------------------------------------------------------
# AppConfig._save_last_config — writes when config in config dir (L529)
# ---------------------------------------------------------------------------

class TestSaveLastConfigWrites:
    def test_saves_when_config_in_config_dir(self, tmp_path):
        """_save_last_config should write the file when config is in _CONFIG_DIR."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        last_file = config_dir / ".last_config"
        cfg_path = config_dir / "myenv.yml"
        cfg_path.write_text("servers: []\n")
        with patch("ssh_ops.config._CONFIG_DIR", config_dir), \
             patch("ssh_ops.config._LAST_CONFIG_FILE", last_file):
            AppConfig._save_last_config(cfg_path)
        assert last_file.exists()
        assert last_file.read_text(encoding="utf-8").strip() == str(cfg_path)


# ---------------------------------------------------------------------------
# AppConfig.__init__ with master_password — triggers _auto_encrypt_and_decrypt (L551)
# AppConfig._auto_encrypt_and_decrypt (L557-571)
# ---------------------------------------------------------------------------

class TestAutoEncryptAndDecrypt:
    def test_init_with_master_password_calls_auto_encrypt(self, tmp_path):
        """__init__ with master_password should call _auto_encrypt_and_decrypt."""
        cfg_path = tmp_path / "test.yml"
        cfg_path.write_text("servers: []\ntasks: []\n", encoding="utf-8")
        with patch.object(AppConfig, "_auto_encrypt_and_decrypt") as mock_aed:
            # Still need _load to work so we don't crash
            mock_aed.side_effect = lambda: None
            config = AppConfig.__new__(AppConfig)
            config.config_path = cfg_path.resolve()
            config._master_password = "secret"
            with patch("ssh_ops.config._load_global_config", return_value={}):
                import yaml as _yaml
                with open(cfg_path, "r", encoding="utf-8") as f:
                    env_raw = _yaml.safe_load(f) or {}
                from ssh_ops.config import _resolve_deep, _deep_merge, _load_global_config as _lgc
                raw = _deep_merge({}, env_raw)
                config._load(raw)
            mock_aed.assert_not_called()  # we called _load manually

    def test_auto_encrypt_and_decrypt_encrypts_and_loads(self, tmp_path):
        """_auto_encrypt_and_decrypt: encrypts plaintext passwords, then loads decrypted config."""
        cfg_path = tmp_path / "test.yml"
        cfg_path.write_text("servers:\n  - host: 10.0.0.1\n    password: plaintext\ntasks: []\n", encoding="utf-8")

        fake_salt = b"saltsaltsaltsalt"
        encrypted_text = "servers:\n  - host: 10.0.0.1\n    password: ENC[abc123]\ntasks: []\n"
        decrypted_config = {"servers": [{"host": "10.0.0.1", "password": "plaintext"}], "tasks": []}

        with patch("ssh_ops.config._load_global_config", return_value={}), \
             patch("ssh_ops.crypto._get_salt", return_value=fake_salt), \
             patch("ssh_ops.crypto.encrypt_passwords_in_yaml", return_value=encrypted_text) as mock_enc, \
             patch("ssh_ops.crypto.decrypt_passwords_in_config", return_value=decrypted_config) as mock_dec:
            config = AppConfig(cfg_path, master_password="secret")

        mock_enc.assert_called_once()
        mock_dec.assert_called_once()
        # encrypted_text != original text, so file should have been written
        written = cfg_path.read_text(encoding="utf-8")
        assert written == encrypted_text
        assert config.servers[0].host == "10.0.0.1"

    def test_auto_encrypt_no_change_skips_write(self, tmp_path):
        """When encrypted_text == raw_text, file is NOT rewritten."""
        cfg_path = tmp_path / "test.yml"
        original_text = "servers: []\ntasks: []\n"
        cfg_path.write_text(original_text, encoding="utf-8")

        fake_salt = b"saltsaltsaltsalt"
        decrypted_config = {"servers": [], "tasks": []}

        with patch("ssh_ops.config._load_global_config", return_value={}), \
             patch("ssh_ops.crypto._get_salt", return_value=fake_salt), \
             patch("ssh_ops.crypto.encrypt_passwords_in_yaml", return_value=original_text), \
             patch("ssh_ops.crypto.decrypt_passwords_in_config", return_value=decrypted_config):
            config = AppConfig(cfg_path, master_password="secret")

        # File content should remain unchanged
        assert cfg_path.read_text(encoding="utf-8") == original_text

    def test_reload_with_master_password_calls_auto_encrypt(self, tmp_path):
        """reload() with master_password set should call _auto_encrypt_and_decrypt."""
        cfg_path = tmp_path / "test.yml"
        cfg_path.write_text("servers: []\ntasks: []\n", encoding="utf-8")
        fake_salt = b"saltsaltsaltsalt"
        decrypted_config = {"servers": [], "tasks": []}

        with patch("ssh_ops.config._load_global_config", return_value={}), \
             patch("ssh_ops.crypto._get_salt", return_value=fake_salt), \
             patch("ssh_ops.crypto.encrypt_passwords_in_yaml", return_value="servers: []\ntasks: []\n"), \
             patch("ssh_ops.crypto.decrypt_passwords_in_config", return_value=decrypted_config):
            config = AppConfig(cfg_path, master_password="secret")

            with patch.object(config, "_auto_encrypt_and_decrypt") as mock_aed:
                config.reload()
                mock_aed.assert_called_once()


# ---------------------------------------------------------------------------
# AppConfig._load — server_warn_patterns (L595) and aliases (L614-616)
# ---------------------------------------------------------------------------

class TestAppConfigLoad:
    def test_server_warn_patterns_loaded(self, tmp_path):
        """Valid server_warn_patterns in safety section are loaded."""
        data = {
            "servers": [],
            "tasks": [],
            "safety": {
                "server_warn_patterns": [
                    {"pattern": "prod*", "label": "PROD", "color": "#ff0000"},
                ]
            },
        }
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        assert len(config.server_warn_patterns) == 1
        assert config.server_warn_patterns[0].pattern == "prod*"
        assert config.server_warn_patterns[0].label == "PROD"

    def test_server_warn_pattern_empty_pattern_skipped(self, tmp_path):
        """Entries with empty pattern are skipped."""
        data = {
            "servers": [],
            "tasks": [],
            "safety": {
                "server_warn_patterns": [
                    {"pattern": "", "label": "WARN"},
                    {"pattern": "prod*", "label": "PROD"},
                ]
            },
        }
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        assert len(config.server_warn_patterns) == 1
        assert config.server_warn_patterns[0].pattern == "prod*"

    def test_aliases_loaded(self, tmp_path):
        """aliases section is parsed into config.aliases dict."""
        data = {
            "servers": [],
            "tasks": [],
            "aliases": {
                "status": "systemctl status nginx",
                "logs": "journalctl -u nginx -n 50",
            },
        }
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        assert config.aliases["status"] == "systemctl status nginx"
        assert config.aliases["logs"] == "journalctl -u nginx -n 50"

    def test_aliases_empty_by_default(self, tmp_path):
        """When no aliases section, config.aliases is empty."""
        data = {"servers": [], "tasks": []}
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        assert config.aliases == {}


# ---------------------------------------------------------------------------
# AppConfig.get_server_warn (L677-688)
# ---------------------------------------------------------------------------

class TestGetServerWarn:
    def _make_config_with_patterns(self, tmp_path):
        data = {
            "servers": [],
            "tasks": [],
            "safety": {
                "server_warn_patterns": [
                    {"pattern": "prod*", "label": "PROD", "color": "#ff0000"},
                    {"pattern": "*.prod.*", "label": "PROD-HOST", "color": "#cc0000"},
                ]
            },
        }
        cfg_path = _write_config(tmp_path, data)
        return AppConfig(cfg_path)

    def test_matches_by_server_name(self, tmp_path):
        config = self._make_config_with_patterns(tmp_path)
        result = config.get_server_warn("prod-web1")
        assert result is not None
        assert result["label"] == "PROD"

    def test_matches_by_host(self, tmp_path):
        config = self._make_config_with_patterns(tmp_path)
        result = config.get_server_warn("someserver", host="app.prod.example.com")
        assert result is not None
        assert result["label"] == "PROD-HOST"

    def test_no_match_returns_none(self, tmp_path):
        config = self._make_config_with_patterns(tmp_path)
        result = config.get_server_warn("dev-web1", host="dev.example.com")
        assert result is None

    def test_case_insensitive_match(self, tmp_path):
        config = self._make_config_with_patterns(tmp_path)
        result = config.get_server_warn("PROD-DB")
        assert result is not None


# ---------------------------------------------------------------------------
# AppConfig.get_safety_settings (L688)
# ---------------------------------------------------------------------------

class TestGetSafetySettings:
    def test_get_safety_settings_structure(self, tmp_path):
        """get_safety_settings returns a dict with expected keys."""
        data = {
            "servers": [],
            "tasks": [],
            "safety": {
                "server_warn_patterns": [
                    {"pattern": "prod*", "label": "PROD", "color": "#ff0000"}
                ],
                "auto_backup": {"enabled": True, "backup_dir": "/bak", "commands": ["rm"]},
                "dest_check": {"enabled": False, "commands": ["cp"]},
                "disabled_plugins": ["interactive_check"],
            },
        }
        cfg_path = _write_config(tmp_path, data)
        config = AppConfig(cfg_path)
        result = config.get_safety_settings()
        assert "server_warn_patterns" in result
        assert result["server_warn_patterns"][0]["pattern"] == "prod*"
        assert result["auto_backup"]["enabled"] is True
        assert result["auto_backup"]["backup_dir"] == "/bak"
        assert result["dest_check"]["enabled"] is False
        assert result["disabled_plugins"] == ["interactive_check"]


# ---------------------------------------------------------------------------
# yaml_transaction — None/empty file branch (line 59-60 of yaml_util.py)
# ---------------------------------------------------------------------------

class TestYamlTransaction:
    def test_yaml_transaction_empty_file_loads_as_none(self, tmp_path):
        """yaml_transaction should treat an empty (None-loading) YAML file as {}."""
        empty_yaml = tmp_path / "empty.yml"
        empty_yaml.write_text("", encoding="utf-8")

        with yaml_transaction(empty_yaml) as raw:
            # raw should be a dict (not None), so we can safely assign
            raw["inserted"] = "value"

        # Verify the file was written with the new key
        content = empty_yaml.read_text(encoding="utf-8")
        assert "inserted" in content
        assert "value" in content
