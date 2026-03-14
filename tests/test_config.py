"""Tests for ssh_ops.config — parsing, validation, env var resolution."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from ssh_ops.config import (
    AppConfig,
    AutoBackupConfig,
    ProxyConfig,
    ServerConfig,
    TaskConfig,
    check_interactive_command,
    is_modifying_command,
    wrap_backup_command,
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
        f"if {s}mkdir -p {backup_dir} && {s}cp -a {target} {bak}; then "
        f"echo '[backup] {target} -> {backup_dir}'; "
        f"{work_cmd}; "
        f"else echo '[WARN] backup failed for {target} — command aborted'; fi"
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
