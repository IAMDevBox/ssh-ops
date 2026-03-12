"""Configuration loader — reads YAML config and resolves environment variables."""

import os
import re
from pathlib import Path
from typing import Any

import yaml  # used by safe_load for config reading


_CONFIG_DIR = Path(__file__).parent.parent / "config"
DEFAULT_CONFIG = _CONFIG_DIR / "default.yml"
_LAST_CONFIG_FILE = _CONFIG_DIR / ".last_config"
DEFAULT_KEY_PATHS = ["~/.ssh/id_rsa", "~/.ssh/id_ed25519"]

# Commands that are always interactive (require a TTY)
_INTERACTIVE_COMMANDS = {
    "top", "htop", "iotop", "atop", "nmon", "glances",
    "vi", "vim", "nvim", "nano", "emacs", "pico", "joe", "mcedit",
    "less", "more", "man",
    "ssh", "telnet", "ftp", "sftp",
    "screen", "tmux", "byobu",
    "mc",
}

# Patterns that indicate interactive usage
_INTERACTIVE_PATTERNS = [
    (re.compile(r'\btail\b.*\s-[^\s]*f'), "tail -f (follow mode) is interactive"),
    (re.compile(r'\bping\b(?!.*\s-c\b)'), "ping without -c runs forever"),
    (re.compile(r'\bwatch\b'), "watch is interactive"),
]


def _base_cmd_name(cmd_path: str) -> str:
    """Extract base command name, handling both / and \\ path separators."""
    return cmd_path.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]


_SHELL_SPLIT_RE = re.compile(r'\s*(?:\|\||&&|[|;])\s*')


def check_interactive_command(command: str) -> str | None:
    """Check if a command is interactive. Returns error message or None if OK.

    Also checks sub-commands in pipes and chains (|, &&, ||, ;).
    """
    if not command:
        return None

    # Split on shell operators to check each sub-command
    sub_commands = _SHELL_SPLIT_RE.split(command.strip())
    for sub in sub_commands:
        sub = sub.strip()
        if not sub:
            continue
        parts = sub.split()
        if not parts:  # pragma: no cover — strip() already handles this
            continue
        base_cmd = _base_cmd_name(parts[0])

        if base_cmd in _INTERACTIVE_COMMANDS:
            return f"'{base_cmd}' is an interactive command and cannot be run via SSH Ops"

    for pattern, msg in _INTERACTIVE_PATTERNS:
        if pattern.search(command):
            return msg + " — not allowed via SSH Ops"

    return None


# Commands/patterns that modify the system
_MODIFYING_COMMANDS = {
    "rm", "rmdir", "mv", "cp", "chmod", "chown", "chgrp",
    "kill", "killall", "pkill",
    "reboot", "shutdown", "halt", "poweroff", "init",
    "mkfs", "fdisk", "parted", "dd",
    "userdel", "useradd", "usermod", "groupdel", "groupadd",
    "iptables", "firewall-cmd", "ufw",
    "yum", "apt", "apt-get", "dnf", "rpm", "dpkg", "pip", "pip3",
}

_MODIFYING_PATTERNS = [
    re.compile(r'\bsystemctl\s+(restart|stop|start|enable|disable|reload)\b'),
    re.compile(r'\bservice\s+\S+\s+(restart|stop|start)\b'),
    re.compile(r'\brm\s'),
    re.compile(r'\bmkdir\b'),
    re.compile(r'\btee\b'),
    re.compile(r'>[^>]'),  # redirect overwrite (but not >>)
]


def is_modifying_command(command: str) -> bool:
    """Check if a command modifies the system."""
    if not command:
        return False
    parts = command.strip().split()
    base_cmd = _base_cmd_name(parts[0])
    if base_cmd in _MODIFYING_COMMANDS:
        return True
    # sudo + modifying command
    if base_cmd == "sudo" and len(parts) > 1:
        sudo_cmd = _base_cmd_name(parts[1])
        if sudo_cmd in _MODIFYING_COMMANDS:
            return True
    for pattern in _MODIFYING_PATTERNS:
        if pattern.search(command):
            return True
    return False


def _resolve_env_vars(value: str) -> str:
    """Replace $VAR or ${VAR} with environment variable values."""
    if not isinstance(value, str):
        return value

    def replacer(match):
        var_name = match.group(1) or match.group(2)
        return os.environ.get(var_name, match.group(0))

    return re.sub(r'\$\{(\w+)\}|\$(\w+)', replacer, value)


def _resolve_deep(obj: Any) -> Any:
    """Recursively resolve environment variables in config values."""
    if isinstance(obj, str):
        return _resolve_env_vars(obj)
    if isinstance(obj, dict):
        return {k: _resolve_deep(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_deep(item) for item in obj]
    return obj



def _auto_detect_key() -> str | None:
    """Find the first existing SSH key file."""
    for key_path in DEFAULT_KEY_PATHS:
        p = Path(key_path).expanduser()
        if p.exists():
            return str(p)
    return None


def _parse_server(entry) -> dict:
    """Parse server entry — supports both dict and shorthand string format.

    Shorthand: "user@host" or "user@host:port"
    """
    if isinstance(entry, str):
        port = 22
        if "@" in entry:
            username, host_part = entry.split("@", 1)
        else:
            username = None
            host_part = entry
        if ":" in host_part:
            host, port_str = host_part.rsplit(":", 1)
            try:
                port = int(port_str)
            except ValueError:
                raise ValueError(f"Invalid port in server shorthand '{entry}': '{port_str}' is not a number")
        else:
            host = host_part
        d = {"host": host, "port": port}
        if username:
            d["username"] = username
        return d
    return entry


class ProxyConfig:
    """PSMP or similar SSH proxy configuration."""

    VALID_TYPES = {"psmp"}

    def __init__(self, data: dict):
        self.type = data.get("type", "psmp")
        if self.type not in self.VALID_TYPES:
            raise ValueError(f"Unknown proxy type: '{self.type}' (supported: {', '.join(self.VALID_TYPES)})")
        self.host = data["host"]
        self.port = data.get("port", 22)
        self.user = data["user"]        # corporate user ID
        self.account = data["account"]  # target server account
        self.auth = data.get("auth", "otp")

    def ssh_username(self, target_host: str) -> str:
        """Build the compound PSMP username: user@account@target_host"""
        return f"{self.user}@{self.account}@{target_host}"

    def __repr__(self):
        return f"Proxy({self.type}, {self.user}@{self.host})"


class ServerConfig:
    def __init__(self, data: dict, proxy: "ProxyConfig | None" = None):
        self.host = data["host"]
        self.name = data.get("name", self.host)
        self.port = data.get("port", 22)
        self.username = data.get("username", os.environ.get("USER", "root"))
        self.password = data.get("password")
        self.key_file = data.get("key_file")
        self.groups = data.get("groups", [])
        self.proxy = proxy

        # Auto-detect SSH key if no password, no key_file, and no proxy
        if not self.password and not self.key_file and not self.proxy:
            self.key_file = _auto_detect_key()

    @property
    def needs_otp(self) -> bool:
        return self.proxy is not None and self.proxy.auth == "otp"

    def __repr__(self):
        return f"Server({self.name}@{self.host}:{self.port})"


class TaskConfig:
    _counter: dict[str, int] = {}

    _VALID_TYPES = {"command", "upload", "script"}

    def __init__(self, data: dict):
        # Auto-detect type from fields
        if "type" in data:
            self.type = data["type"]
        elif "src" in data and "dest" in data:
            self.type = "upload"
        elif "command" in data:
            self.type = "command"
        else:
            self.type = "command"

        if "name" in data:
            self.name = data["name"]
        else:
            TaskConfig._counter[self.type] = TaskConfig._counter.get(self.type, 0) + 1
            self.name = f"{self.type}-{TaskConfig._counter[self.type]}"

        # Build a descriptive label for error messages (more helpful than auto-names)
        if data.get("command"):
            self._desc = data["command"]
        elif data.get("src") and data.get("dest"):
            self._desc = f"{data['src']} -> {data['dest']}"
        elif data.get("src"):
            self._desc = data["src"]
        else:
            self._desc = self.name

        # Validate type
        if self.type not in self._VALID_TYPES:
            raise ValueError(f"Task '{self._desc}': unknown type '{self.type}' (must be command/upload/script)")

        self.command = data.get("command")
        self.src = data.get("src")
        self.dest = data.get("dest")
        self.mode = data.get("mode")
        self.args = data.get("args", "")

        # Validate required fields per type
        if self.type == "command":
            if not self.command or not str(self.command).strip():
                raise ValueError(f"Task '{self._desc}': command is required and cannot be empty")
        elif self.type == "upload":
            if not self.src:
                raise ValueError(f"Task '{self._desc}': src is required for upload task")
            if not self.dest:
                raise ValueError(f"Task '{self._desc}': dest is required for upload task")
        elif self.type == "script":
            if not self.src:
                raise ValueError(f"Task '{self._desc}': src is required for script task")

        # Validate interactive commands
        if self.command:
            err = check_interactive_command(str(self.command).strip())
            if err:
                raise ValueError(f"Task '{self._desc}': {err}")

        # Validate src: must be absolute file path, not a directory
        if self.src:
            if not (self.src.startswith("/") or (len(self.src) >= 3 and self.src[1] == ":" and self.src[2] in "/\\")):
                raise ValueError(f"Task '{self._desc}': src must be an absolute path, got '{self.src}'")
            if self.src.endswith("/") or self.src.endswith("\\"):
                raise ValueError(f"Task '{self._desc}': src must be a file path, not a directory: '{self.src}'")

        # Validate dest: must be absolute path and not a directory (no trailing /)
        if self.dest:
            if not self.dest.startswith("/"):
                raise ValueError(f"Task '{self._desc}': dest must be an absolute path, got '{self.dest}'")
            if self.dest.endswith("/") or self.dest.endswith("\\"):
                raise ValueError(f"Task '{self._desc}': dest must be a file path, not a directory: '{self.dest}'")

        # Validate timeout
        timeout_raw = data.get("timeout", 120)
        try:
            self.timeout = int(timeout_raw)
            if self.timeout <= 0:
                raise ValueError("must be positive")
        except (TypeError, ValueError):
            raise ValueError(f"Task '{self._desc}': timeout must be a positive integer, got '{timeout_raw}'")

        # Validate mode (octal)
        if self.mode is not None:
            try:
                if isinstance(self.mode, int):
                    if self.mode < 0 or self.mode > 0o7777:
                        raise ValueError("out of range")
                else:
                    val = int(str(self.mode), 8)
                    if val < 0 or val > 0o7777:
                        raise ValueError("out of range")
            except ValueError:
                raise ValueError(f"Task '{self._desc}': mode must be a valid octal value (e.g. 0755), got '{self.mode}'")

        self.env = data.get("env", {})


    def __repr__(self):
        return f"Task({self.name}, type={self.type})"


class AppConfig:
    @staticmethod
    def default_config_path() -> Path:
        """Return the last used config path, or default.yml if none saved."""
        if _LAST_CONFIG_FILE.exists():
            saved = _LAST_CONFIG_FILE.read_text(encoding="utf-8").strip()
            p = Path(saved)
            if p.exists():
                return p
        return DEFAULT_CONFIG

    @staticmethod
    def _save_last_config(config_path: Path):
        """Save the current config path for next startup.

        Only saves if the config file is in the real config directory,
        preventing test runs from polluting the user's environment.
        """
        try:
            if config_path.parent.resolve() != _CONFIG_DIR.resolve():
                return
            _LAST_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
            _LAST_CONFIG_FILE.write_text(str(config_path), encoding="utf-8")
        except OSError:
            pass

    def __init__(self, config_path: str | Path | None = None):
        if config_path is None:  # pragma: no cover — CLI always provides a path
            config_path = DEFAULT_CONFIG
        self.config_path = Path(config_path).resolve()

        with open(self.config_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        raw = _resolve_deep(raw)

        self._load(raw)

    def _load(self, raw: dict):
        """Load config from parsed YAML dict, resetting task counter."""
        TaskConfig._counter.clear()

        # Global proxy (e.g. PSMP) — shared by all servers
        proxy_data = raw.get("proxy")
        self.proxy = ProxyConfig(proxy_data) if proxy_data else None

        self.servers = [
            ServerConfig(_parse_server(s), proxy=self.proxy)
            for s in raw.get("servers", [])
        ]
        self.tasks = [TaskConfig(t) for t in raw.get("tasks", [])]

        # Command aliases: short name -> full command
        self.aliases: dict[str, str] = {}
        for name, cmd in raw.get("aliases", {}).items():
            self.aliases[str(name)] = str(cmd)

        settings = raw.get("settings", {})
        self.log_dir = Path(
            settings.get("log_dir", "./logs")
        ).resolve()
        self.default_timeout = settings.get("default_timeout", 120)
        self.keep_alive = settings.get("keep_alive", 60)
        self.web_host = settings.get("web_host", "127.0.0.1")
        self.web_port = settings.get("web_port", 8080)

        self.log_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _validate_raw(raw: dict):
        """Validate parsed YAML structure without mutating any state.
        Raises ValueError on invalid config.
        """
        if not isinstance(raw, dict):
            raise ValueError("Config must be a YAML mapping")
        # Validate servers
        for i, s in enumerate(raw.get("servers", [])):
            if isinstance(s, str):
                continue  # shorthand like "user@host:port"
            if not isinstance(s, dict):
                raise ValueError(f"servers[{i}]: must be a mapping or string")
            if "host" not in s:
                raise ValueError(f"servers[{i}]: missing 'host'")
        # Validate tasks
        for i, t in enumerate(raw.get("tasks", [])):
            if not isinstance(t, dict):
                raise ValueError(f"tasks[{i}]: must be a mapping")

    def get_server(self, name: str) -> ServerConfig | None:
        for s in self.servers:
            if s.name == name:
                return s
        return None

    def get_servers_by_group(self, group: str) -> list[ServerConfig]:
        return [s for s in self.servers if group in s.groups]

    def get_task(self, name: str) -> TaskConfig | None:
        for t in self.tasks:
            if t.name == name:
                return t
        return None

    def filter_servers(self, names: list[str] | None = None,
                       group: str | None = None) -> list[ServerConfig]:
        """Filter servers by name list or group. Returns all if no filter."""
        if names:
            return [s for s in self.servers if s.name in names]
        if group:
            return self.get_servers_by_group(group)
        return self.servers

    def reload(self):
        """Re-read config file and update all attributes in place."""
        with open(self.config_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        raw = _resolve_deep(raw)
        self._load(raw)

    def switch_config(self, config_path: str | Path):
        """Switch to a different config file and reload."""
        self.config_path = Path(config_path).resolve()
        self.reload()
        self._save_last_config(self.config_path)

    @staticmethod
    def list_configs(config_dir: Path | None = None) -> list[dict]:
        """List available .yml config files in the config directory, sorted by last modified time (newest first)."""
        if config_dir is None:
            config_dir = Path(__file__).parent.parent / "config"
        configs = []
        if config_dir.exists():
            for f in config_dir.glob("*.yml"):
                configs.append({"name": f.stem, "path": str(f.resolve()), "mtime": f.stat().st_mtime})
            for f in config_dir.glob("*.yaml"):
                configs.append({"name": f.stem, "path": str(f.resolve()), "mtime": f.stat().st_mtime})
        configs.sort(key=lambda c: c["mtime"], reverse=True)
        return configs
