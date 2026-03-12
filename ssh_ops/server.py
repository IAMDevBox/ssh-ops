"""Web server — FastAPI + WebSocket for real-time SSH operations."""

import asyncio
import json
import shutil
import tempfile
import threading
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, Form
from fastapi.responses import HTMLResponse, PlainTextResponse

import yaml as _pyyaml
from ruamel.yaml import YAMLError as _RuamelYAMLError

from ssh_ops.yaml_util import load_yaml, dump_yaml, dump_yaml_str, safe_load_str

from .config import AppConfig, TaskConfig, check_interactive_command, is_modifying_command
from .executor import ConnectionPool, TaskExecutor, _exhaust_generator, _shell_quote
from .logger import ExecLogger

MAX_HISTORY = 5000  # max lines to keep in memory


def _friendly_error(e: Exception) -> str:
    """Convert exceptions to user-friendly error messages."""
    msg = str(e)
    if isinstance(e, (_RuamelYAMLError, _pyyaml.YAMLError)):
        if hasattr(e, 'problem_mark'):
            mark = e.problem_mark
            return f"YAML syntax error at line {mark.line + 1}, column {mark.column + 1}: {e.problem}"
        return f"YAML syntax error: {msg}"
    return msg
MAX_CMD_HISTORY = 50
MAX_UPLOAD_HISTORY = 50


def create_app(config: AppConfig, logger: ExecLogger) -> FastAPI:
    pool = ConnectionPool(config.keep_alive)
    executor = TaskExecutor(pool, logger)
    ws_clients: set[WebSocket] = set()
    output_history: deque[str] = deque(maxlen=MAX_HISTORY)
    cmd_history: deque[str] = deque(maxlen=MAX_CMD_HISTORY)
    upload_history: deque[dict] = deque(maxlen=MAX_UPLOAD_HISTORY)

    def _history_file() -> Path:
        return config.config_path.parent / ".history" / (config.config_path.stem + ".json")

    def _load_history():
        cmd_history.clear()
        upload_history.clear()
        f = _history_file()
        if f.exists():
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                for item in data.get("cmd", []):
                    cmd_history.append(item)
                for item in data.get("upload", []):
                    upload_history.append(item)
            except Exception:
                pass

    def _save_history():
        f = _history_file()
        f.parent.mkdir(parents=True, exist_ok=True)
        data = {"cmd": list(cmd_history), "upload": list(upload_history)}
        f.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

    _load_history()
    _main_loop: asyncio.AbstractEventLoop | None = None
    _last_server_status: dict[str, bool] = {}

    def _get_server_status() -> dict[str, bool]:
        connected = set(pool.connected_servers())
        return {s.name: s.name in connected for s in config.servers}

    async def _broadcast_server_status():
        status = _get_server_status()
        if status != _last_server_status:
            _last_server_status.clear()
            _last_server_status.update(status)
            await _broadcast(json.dumps({"type": "server_status", "servers": status}))

    async def _server_status_monitor():  # pragma: no cover
        while True:
            await asyncio.sleep(5)
            try:
                await _broadcast_server_status()
            except Exception:
                pass

    @asynccontextmanager
    async def lifespan(_app):  # pragma: no cover — triggered by real server, not TestClient
        nonlocal _main_loop
        _main_loop = asyncio.get_running_loop()
        task = asyncio.create_task(_server_status_monitor())
        yield
        task.cancel()

    app = FastAPI(title="SSH Ops Tool", lifespan=lifespan)

    def _broadcast_from_thread(message: str):  # pragma: no cover — called from background threads at runtime
        """Thread-safe broadcast: schedule _broadcast on the main event loop."""
        loop = _main_loop
        if loop is not None and loop.is_running():
            asyncio.run_coroutine_threadsafe(_broadcast(message), loop)

    # Task execution progress state — 2D matrix: servers × tasks
    _task_status: dict = {"running": False}
    _cancel_event: threading.Event = threading.Event()

    def _progress_callback(event: dict):
        """Called by executor during task execution to update progress state and broadcast."""
        srv = event.get("server", "")
        if event["event"] == "server_start":
            _task_status["current_server"] = srv
            # Initialize all tasks as pending for this server
            task_names = _task_status.get("task_names", [])
            _task_status["servers"][srv] = {t: {"status": "pending"} for t in task_names}
        elif event["event"] == "task_start":
            _task_status["servers"].setdefault(srv, {})[event["task"]] = {"status": "running"}
        elif event["event"] == "task_done":
            if event.get("cancelled"):
                _task_status["servers"].setdefault(srv, {})[event["task"]] = {
                    "status": "cancelled"}
            else:
                _task_status["servers"].setdefault(srv, {})[event["task"]] = {
                    "status": "done", "success": event["success"]}
        # Broadcast progress as JSON
        _broadcast_from_thread(json.dumps({"type": "progress", **_task_status}))

    # --- HTML UI ---
    @app.get("/", response_class=HTMLResponse)
    async def index():
        template_path = Path(__file__).parent / "templates" / "index.html"
        return template_path.read_text(encoding="utf-8")

    # --- REST API ---
    @app.get("/api/task-status")
    async def get_task_status():
        """Return current task execution status."""
        return _task_status

    @app.post("/api/cancel")
    async def cancel_tasks():
        """Cancel remaining tasks. Running task will finish, subsequent ones are skipped."""
        if not _task_status.get("running"):
            return {"status": "not_running"}
        _cancel_event.set()
        _task_status["cancelling"] = True
        await _broadcast(json.dumps({"type": "progress", **_task_status}))
        await _broadcast("⚠ Cancelling — remaining tasks will be skipped")
        return {"status": "cancelling"}

    @app.get("/api/proxy-info")
    async def proxy_info():
        """Return proxy config info (without secrets)."""
        if config.proxy:
            return {
                "enabled": True,
                "type": config.proxy.type,
                "host": config.proxy.host,
                "user": config.proxy.user,
                "account": config.proxy.account,
                "auth": config.proxy.auth,
            }
        return {"enabled": False}

    @app.post("/api/update-proxy")
    async def update_proxy(body: dict):
        """Update or remove the global proxy config.

        Body: {"host":"...", "user":"...", "account":"..."} to set,
              {"remove": true} to remove.
        """
        try:
            raw = load_yaml(config.config_path) or {}

            if body.get("remove"):
                raw.pop("proxy", None)
                dump_yaml(raw, config.config_path)
                pool.disconnect_all()
                config.reload()
                await _broadcast("Proxy removed")
                return {"status": "ok"}

            host = (body.get("host") or "").strip()
            user = (body.get("user") or "").strip()
            account = (body.get("account") or "").strip()
            if not host or not user or not account:
                return {"error": "host, user, and account are required"}

            raw["proxy"] = {
                "type": "psmp",
                "host": host,
                "user": user,
                "account": account,
                "auth": "otp",
            }
            dump_yaml(raw, config.config_path)
            pool.disconnect_all()
            config.reload()
            await _broadcast(f"Proxy configured: {user}@{account}@*@{host}")
            return {"status": "ok"}
        except Exception as e:
            return {"error": _friendly_error(e)}

    @app.get("/api/servers")
    async def list_servers():
        return [
            {
                "name": s.name,
                "host": s.host,
                "port": s.port,
                "username": s.username,
                "groups": s.groups,
                "connected": s.name in pool.connected_servers(),
                "needs_otp": s.needs_otp,
            }
            for s in config.servers
        ]

    @app.get("/api/tasks")
    async def list_tasks():
        return [
            {
                "index": i,
                "name": t.name,
                "type": t.type,
                "command": t.command,
                "src": t.src,
                "dest": t.dest,
                "mode": t.mode,
                "timeout": t.timeout,
                "env": t.env or None,
                "modifying": is_modifying_command(t.command) if t.command else False,
            }
            for i, t in enumerate(config.tasks)
        ]

    @app.get("/api/aliases")
    async def list_aliases():
        return config.aliases

    @app.post("/api/update-aliases")
    async def update_aliases(body: dict):
        """Save aliases to config. Body: {"aliases": {"name": "command", ...}}"""
        aliases = body.get("aliases", {})
        if not isinstance(aliases, dict):
            return {"error": "aliases must be a mapping"}
        # Validate: keys and values must be non-empty strings
        for name, cmd in aliases.items():
            if not str(name).strip():
                return {"error": "alias name cannot be empty"}
            if not str(cmd).strip():
                return {"error": f"alias '{name}' has empty command"}
        try:
            raw = load_yaml(config.config_path) or {}
            if aliases:
                raw["aliases"] = {str(k).strip(): str(v).strip() for k, v in aliases.items()}
            else:
                raw.pop("aliases", None)
            dump_yaml(raw, config.config_path)
            config.reload()
            return {"status": "ok"}
        except Exception as e:
            return {"error": _friendly_error(e)}

    @app.get("/api/logs/{server_name}")
    async def get_server_log(server_name: str, date: str = ""):
        """Return today's log for a server. ?date=2026-03-07 for specific date."""
        if not date:
            date = datetime.now().strftime("%Y-%m-%d")
        log_file = logger.log_dir / logger._safe_name(server_name) / f"{date}.log"
        if not log_file.exists():
            return PlainTextResponse(f"No log for {server_name} on {date}")
        return PlainTextResponse(log_file.read_text(encoding="utf-8"))

    @app.get("/api/history")
    async def get_history():
        """Return all buffered output lines."""
        return list(output_history)

    @app.post("/api/clear-history")
    async def clear_history():
        output_history.clear()
        return {"status": "cleared"}

    # --- Config management ---
    @app.get("/api/configs")
    async def list_configs():
        configs = AppConfig.list_configs(config.config_path.parent)
        current = str(config.config_path)
        return {"configs": configs, "current": current}

    @app.get("/api/download-config")
    async def download_config():
        """Download the current config file."""
        from fastapi.responses import FileResponse
        return FileResponse(
            config.config_path,
            filename=config.config_path.name,
            media_type="application/x-yaml",
        )

    @app.post("/api/reload")
    async def reload_config():
        try:
            old_connected = pool.connected_servers()
            config.reload()
            pool.disconnect_all()
            msg = "Config reloaded: " + config.config_path.name
            if old_connected:
                msg += " (disconnected: " + ", ".join(old_connected) + ")"
            await _broadcast(msg)
            return {"status": "ok", "config": str(config.config_path)}
        except Exception as e:
            return {"error": _friendly_error(e)}

    @app.post("/api/switch-config")
    async def switch_config(body: dict):
        path = body.get("path", "")
        if not path:
            return {"error": "no path provided"}
        try:
            resolved = Path(path).resolve()
            config_dir = config.config_path.parent.resolve()
            if not str(resolved).startswith(str(config_dir) + "/") and resolved.parent != config_dir:
                return {"error": "config must be in config directory"}
            old_connected = pool.connected_servers()
            config.switch_config(path)
            pool.disconnect_all()
            _load_history()
            msg = "Switched to config: " + config.config_path.name
            if old_connected:
                msg += " (disconnected: " + ", ".join(old_connected) + ")"
            await _broadcast(msg)
            return {"status": "ok", "config": str(config.config_path)}
        except Exception as e:
            return {"error": _friendly_error(e)}

    @app.post("/api/create-config")
    async def create_config(body: dict):
        """Create a new config file. Body: {"name": "prod", "servers": [...], "tasks": [...]}

        servers: list of {"host":"...", "name":"...", "port":22, "username":"...", "password":"...","groups":[...]}
        tasks: list of {"command":"..."} or {"src":"...","dest":"..."}
        """
        name = body.get("name", "").strip()
        if not name:
            return {"error": "config name is required"}
        if len(name) > 64 or not all(c.isalnum() or c in "-_" for c in name):
            return {"error": "config name must be alphanumeric (hyphens/underscores, max 64 chars)"}

        config_dir = config.config_path.parent
        new_path = config_dir / f"{name}.yml"
        if new_path.exists():
            return {"error": f"config '{name}' already exists"}

        raw_servers = body.get("servers", [])
        raw_tasks = body.get("tasks", [])

        data = {"servers": raw_servers, "tasks": raw_tasks}

        try:
            dump_yaml(data, new_path)
            # Switch to the new config
            old_connected = pool.connected_servers()
            config.switch_config(new_path)
            pool.disconnect_all()
            msg = f"Created and switched to config: {name}"
            if old_connected:
                msg += " (disconnected: " + ", ".join(old_connected) + ")"
            await _broadcast(msg)
            return {"status": "ok", "config": str(new_path)}
        except Exception as e:
            # Clean up if switch failed
            if new_path.exists():
                new_path.unlink()
            return {"error": _friendly_error(e)}

    @app.post("/api/duplicate-config")
    async def duplicate_config(body: dict):
        """Duplicate current config under a new name. Body: {"name": "prod-copy"}"""
        name = body.get("name", "").strip()
        if not name:
            return {"error": "new config name is required"}
        if len(name) > 64 or not all(c.isalnum() or c in "-_" for c in name):
            return {"error": "config name must be alphanumeric (hyphens/underscores, max 64 chars)"}

        config_dir = config.config_path.parent
        new_path = config_dir / f"{name}.yml"
        if new_path.exists():
            return {"error": f"config '{name}' already exists"}

        try:
            # Copy current config content
            with open(config.config_path, "r", encoding="utf-8") as f:
                content = f.read()
            with open(new_path, "w", encoding="utf-8") as f:
                f.write(content)
            # Switch to the duplicated config
            old_connected = pool.connected_servers()
            config.switch_config(new_path)
            pool.disconnect_all()
            msg = f"Duplicated config as '{name}' and switched to it"
            if old_connected:
                msg += " (disconnected: " + ", ".join(old_connected) + ")"
            await _broadcast(msg)
            return {"status": "ok", "config": str(new_path)}
        except Exception as e:
            if new_path.exists():
                new_path.unlink()
            return {"error": _friendly_error(e)}

    @app.post("/api/rename-config")
    async def rename_config(body: dict):
        """Rename the current config file. Body: {"name": "new-name"}"""
        name = body.get("name", "").strip()
        if not name:
            return {"error": "new config name is required"}
        if len(name) > 64 or not all(c.isalnum() or c in "-_" for c in name):
            return {"error": "config name must be alphanumeric (hyphens/underscores, max 64 chars)"}

        old_path = config.config_path
        new_path = old_path.parent / f"{name}.yml"
        if new_path.exists():
            return {"error": f"config '{name}' already exists"}

        try:
            old_connected = pool.connected_servers()
            old_path.rename(new_path)
            config.switch_config(new_path)
            pool.disconnect_all()
            msg = f"Config renamed to '{name}'"
            if old_connected:
                msg += " (disconnected: " + ", ".join(old_connected) + ")"
            await _broadcast(msg)
            return {"status": "ok", "config": str(new_path)}
        except Exception as e:
            # Try to restore if rename succeeded but switch failed
            if new_path.exists() and not old_path.exists():
                new_path.rename(old_path)
            return {"error": _friendly_error(e)}

    @app.post("/api/add-server")
    async def add_server(body: dict):
        """Add a server to current config. Body: {"host":"...", "name":"...", ...}"""
        host = body.get("host", "").strip()
        if not host:
            return {"error": "host is required"}
        entry = {"host": host}
        if body.get("name", "").strip():
            entry["name"] = body["name"].strip()
        if body.get("port"):
            try:
                entry["port"] = int(body["port"])
            except (ValueError, TypeError):
                return {"error": "port must be a number"}
        if body.get("username", "").strip():
            entry["username"] = body["username"].strip()
        if body.get("password", "").strip():
            entry["password"] = body["password"].strip()
        if body.get("groups"):
            groups = body["groups"]
            if isinstance(groups, str):
                groups = [g.strip() for g in groups.split(",") if g.strip()]
            entry["groups"] = groups

        try:
            raw = load_yaml(config.config_path) or {}
            if "servers" not in raw:
                raw["servers"] = []
            raw["servers"].append(entry)
            dump_yaml(raw, config.config_path)
            config.reload()
            detail = entry.get("name") or host
            await _broadcast(f"Server added: {detail}")
            return {"status": "ok", "server": entry}
        except Exception as e:
            return {"error": _friendly_error(e)}

    @app.post("/api/delete-config")
    async def delete_config(body: dict):
        """Delete current config and switch to another. Body: {"confirm": true}"""
        if not body.get("confirm"):
            return {"error": "confirmation required"}

        config_dir = config.config_path.parent
        current_path = config.config_path
        current_name = current_path.stem

        # Find another config to switch to
        others = [f for f in sorted(config_dir.glob("*.yml")) if f.resolve() != current_path.resolve()]
        others += [f for f in sorted(config_dir.glob("*.yaml")) if f.resolve() != current_path.resolve()]
        if not others:
            return {"error": "cannot delete the only config file"}

        try:
            pool.disconnect_all()
            current_path.unlink()
            config.switch_config(others[0])
            await _broadcast(f"Deleted config '{current_name}', switched to '{others[0].stem}'")
            return {"status": "ok", "switched_to": str(others[0])}
        except Exception as e:
            return {"error": _friendly_error(e)}

    @app.post("/api/update-server-password")
    async def update_server_password(body: dict):
        """Update a server's password by index. Body: {"index": 0, "password": "new"}"""
        idx = body.get("index")
        if idx is None:
            return {"error": "no index provided"}
        if not isinstance(idx, int):
            return {"error": "index must be an integer"}
        password = body.get("password")
        if password is not None and not isinstance(password, str):
            return {"error": "password must be a string"}
        if password is not None and not password.strip():
            return {"error": "password cannot be empty; omit the field to keep current password"}

        try:
            raw = load_yaml(config.config_path) or {}
            servers = raw.get("servers", [])
            if idx < 0 or idx >= len(servers):
                return {"error": "index out of range"}
            if password is None:
                return {"status": "ok", "message": "no change"}
            servers[idx]["password"] = password
            dump_yaml(raw, config.config_path)
            server_name = servers[idx].get("name") or servers[idx].get("host", "")
            # Disconnect so next connect uses new password
            pool.disconnect(server_name)
            config.reload()
            await _broadcast(f"Password updated for {server_name}")
            return {"status": "ok", "server": server_name}
        except Exception as e:
            return {"error": _friendly_error(e)}

    @app.post("/api/delete-server")
    async def delete_server(body: dict):
        """Delete a server by index. Body: {"index": 0}"""
        idx = body.get("index")
        if idx is None:
            return {"error": "no index provided"}
        if not isinstance(idx, int):
            return {"error": "index must be an integer"}
        try:
            raw = load_yaml(config.config_path) or {}
            servers = raw.get("servers", [])
            if idx < 0 or idx >= len(servers):
                return {"error": "index out of range"}
            removed = servers.pop(idx)
            dump_yaml(raw, config.config_path)
            # Disconnect removed server if connected
            removed_name = removed.get("name") or removed.get("host", "")
            pool.disconnect(removed_name)
            config.reload()
            await _broadcast(f"Server removed: {removed_name}")
            return {"status": "ok", "removed": removed}
        except Exception as e:
            return {"error": _friendly_error(e)}

    @app.get("/api/tasks-yaml")
    async def get_tasks_yaml():
        """Return the tasks section of the config as YAML text."""
        try:
            raw = load_yaml(config.config_path) or {}
            tasks_section = raw.get("tasks", [])
            text = dump_yaml_str({"tasks": tasks_section})
            return PlainTextResponse(text)
        except Exception as e:
            return PlainTextResponse(f"Error reading config: {e}")

    @app.get("/api/config-yaml")
    async def get_config_yaml(raw: str = ""):
        """Return the full config file content. Passwords masked unless ?raw=1."""
        try:
            text = config.config_path.read_text(encoding="utf-8")
            if raw == "1":
                return PlainTextResponse(text)
            parsed = load_yaml(config.config_path) or {}
            for s in parsed.get("servers", []):
                if "password" in s:
                    s["password"] = "********"
            masked = dump_yaml_str(parsed)
            return PlainTextResponse(masked)
        except Exception as e:
            return PlainTextResponse(f"Error reading config: {e}")

    @app.post("/api/update-config-yaml")
    async def update_config_yaml(body: dict):
        """Save raw YAML to config file with validation and atomic write."""
        yaml_text = body.get("yaml", "")
        if not yaml_text.strip():
            return {"error": "Empty YAML content"}
        # Validate YAML syntax
        try:
            parsed = safe_load_str(yaml_text)
        except Exception as e:
            return {"error": _friendly_error(e)}
        if not isinstance(parsed, dict):
            return {"error": "Config must be a YAML mapping (not a list or scalar)"}
        # Validate semantic structure by attempting to load
        try:
            AppConfig._validate_raw(parsed)
        except Exception as e:
            return {"error": f"Config validation: {e}"}
        # Atomic write: write to temp file, then replace
        cfg_path = config.config_path
        bak_path = cfg_path.with_suffix(cfg_path.suffix + ".bak")
        try:
            # Backup current config
            if cfg_path.exists():
                shutil.copy2(cfg_path, bak_path)
            # Write to temp then atomic replace
            tmp_fd, tmp_path = tempfile.mkstemp(dir=str(cfg_path.parent), suffix=".yml.tmp")
            try:
                with open(tmp_fd, "w", encoding="utf-8") as f:
                    f.write(yaml_text)
                Path(tmp_path).replace(cfg_path)
            except Exception:
                Path(tmp_path).unlink(missing_ok=True)
                raise
            # Reload config
            old_connected = pool.connected_servers()
            config.reload()
            pool.disconnect_all()
            msg = "Config updated and reloaded: " + cfg_path.name
            if old_connected:
                msg += " (disconnected: " + ", ".join(old_connected) + ")"
            await _broadcast(msg)
            return {"status": "ok"}
        except Exception as e:
            return {"error": f"Failed to save: {e}"}

    @app.post("/api/add-task")
    async def add_task(body: dict):
        """Add a task to the config file. Body: {"type":"command","command":"..."} or {"type":"upload","src":"...","dest":"...","mode":"..."}"""
        task_type = body.get("type", "")
        if task_type not in ("command", "upload"):
            return {"error": "type must be 'command' or 'upload'"}

        # Build task entry for YAML
        entry = {}
        if task_type == "command":
            cmd = body.get("command", "").strip()
            if not cmd:
                return {"error": "no command provided"}
            interactive_err = check_interactive_command(cmd)
            if interactive_err:
                return {"error": interactive_err}
            entry["command"] = cmd
        else:
            src = body.get("src", "").strip()
            dest = body.get("dest", "").strip()
            if not src or not dest:
                return {"error": "src and dest required"}
            if not src.startswith("/") and not (len(src) >= 3 and src[1] == ":" and src[2] in "/\\"):
                return {"error": "src must be an absolute path (e.g. /opt/files/app.conf)"}
            if src.endswith("/") or src.endswith("\\"):
                return {"error": "src must be a file path, not a directory"}
            if not dest.startswith("/"):
                return {"error": "dest must be an absolute path (e.g. /opt/app/file.txt)"}
            if dest.endswith("/") or dest.endswith("\\"):
                return {"error": "dest must be a file path, not a directory"}
            entry["src"] = src
            entry["dest"] = dest
            if body.get("mode"):
                entry["mode"] = body["mode"]

        # Read, append, write YAML
        try:
            raw = load_yaml(config.config_path) or {}
            if "tasks" not in raw:
                raw["tasks"] = []
            raw["tasks"].append(entry)
            dump_yaml(raw, config.config_path)
            config.reload()
            detail = entry.get("command") or f"{entry.get('src')} -> {entry.get('dest')}"
            await _broadcast(f"Task added to config: {detail}")
            return {"status": "ok", "task": entry}
        except Exception as e:
            return {"error": _friendly_error(e)}

    @app.post("/api/reorder-task")
    async def reorder_task(body: dict):
        """Move a task. Supports adjacent swap or arbitrary move.

        Adjacent: {"index": 0, "direction": "up"|"down"}
        Arbitrary: {"from": 0, "to": 2}
        """
        try:
            raw = load_yaml(config.config_path) or {}
            tasks = raw.get("tasks", [])

            if "from_index" in body and "to_index" in body:
                frm = body["from_index"]
                to = body["to_index"]
                if not isinstance(frm, int) or not isinstance(to, int):
                    return {"error": "from and to must be integers"}
                if frm < 0 or frm >= len(tasks) or to < 0 or to >= len(tasks):
                    return {"error": "index out of range"}
                if frm == to:
                    return {"status": "ok", "from": frm, "to": to}
                task = tasks.pop(frm)
                # Dragging down: pop shifts indices, adjust target
                insert_at = to - 1 if frm < to else to
                tasks.insert(insert_at, task)
            else:
                idx = body.get("index")
                direction = body.get("direction", "")
                if idx is None or not isinstance(idx, int):
                    return {"error": "index must be an integer"}
                if direction not in ("up", "down"):
                    return {"error": "direction must be 'up' or 'down'"}
                if idx < 0 or idx >= len(tasks):
                    return {"error": "index out of range"}
                new_idx = idx - 1 if direction == "up" else idx + 1
                if new_idx < 0 or new_idx >= len(tasks):
                    return {"error": "already at the edge"}
                tasks[idx], tasks[new_idx] = tasks[new_idx], tasks[idx]
                frm, to = idx, new_idx

            dump_yaml(raw, config.config_path)
            config.reload()
            return {"status": "ok", "from": frm, "to": to}
        except Exception as e:
            return {"error": _friendly_error(e)}

    @app.post("/api/delete-task")
    async def delete_task(body: dict):
        """Delete a task by index. Body: {"index": 0}"""
        idx = body.get("index")
        if idx is None:
            return {"error": "no index provided"}
        if not isinstance(idx, int):
            return {"error": "index must be an integer"}
        try:
            raw = load_yaml(config.config_path) or {}
            tasks = raw.get("tasks", [])
            if idx < 0 or idx >= len(tasks):
                return {"error": "index out of range"}
            removed = tasks.pop(idx)
            dump_yaml(raw, config.config_path)
            config.reload()
            detail = removed.get("command") or f"{removed.get('src')} -> {removed.get('dest')}"
            await _broadcast(f"Task removed from config: {detail}")
            return {"status": "ok", "removed": removed}
        except Exception as e:
            return {"error": _friendly_error(e)}

    # --- Command / upload history ---
    @app.get("/api/cmd-history")
    async def get_cmd_history():
        return list(cmd_history)

    @app.post("/api/delete-cmd-history")
    async def delete_cmd_history(body: dict):
        """Delete one or all command history items. Body: {"command": "..."} or {"all": true}"""
        if body.get("all"):
            cmd_history.clear()
            _save_history()
            return {"status": "cleared"}
        cmd = body.get("command", "")
        if cmd in cmd_history:
            cmd_history.remove(cmd)
            _save_history()
        return {"status": "ok"}

    @app.get("/api/upload-history")
    async def get_upload_history():
        return list(upload_history)

    @app.post("/api/delete-upload-history")
    async def delete_upload_history(body: dict):
        """Delete one or all upload history items. Body: {"dest": "...", "filename": "..."} or {"all": true}"""
        if body.get("all"):
            upload_history.clear()
            _save_history()
            return {"status": "cleared"}
        dest = body.get("dest", "")
        filename = body.get("filename", "")
        for existing in list(upload_history):
            if existing["dest"] == dest and existing["filename"] == filename:
                upload_history.remove(existing)
                _save_history()
                break
        return {"status": "ok"}

    @app.post("/api/connect")
    async def connect_servers(body: dict):
        """Connect to servers. Body: {"servers": [...], "otp": "..."} or {"all": true, "otp": "..."}"""
        results = {}
        otp = body.get("otp")  # OTP for PSMP servers (shared across all)

        if body.get("all"):
            targets = config.servers
        else:
            names = body.get("servers", [])
            targets = [s for s in config.servers if s.name in names]

        # Check if OTP is needed but not provided
        needs_otp = any(s.needs_otp for s in targets)
        if needs_otp and not otp:
            return {"error": "OTP required for PSMP connection", "needs_otp": True}

        import concurrent.futures

        def _connect_one(server):
            was_alive = pool.get_session(server.name) is not None
            pool.connect(server, otp=otp if server.needs_otp else None)
            return "already_connected" if was_alive else "connected"

        # Connect in parallel so OTP doesn't expire for later servers
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(targets)) as ex:
            futures = {ex.submit(_connect_one, s): s for s in targets}
            for future in concurrent.futures.as_completed(futures):
                server = futures[future]
                try:
                    results[server.name] = future.result()
                except Exception as e:
                    results[server.name] = f"error: {e}"

        # Broadcast results after all connections complete
        for server in targets:
            msg = results.get(server.name, "unknown")
            if msg == "already_connected":
                await _broadcast(f"[{server.name}] Already connected")
            elif msg == "connected":
                await _broadcast(f"[{server.name}] Connected")
                # Show MOTD/banner if available
                session = pool.get_session(server.name)
                if session and getattr(session, 'motd', ''):
                    for line in session.motd.splitlines():
                        if line.strip():
                            await _broadcast(f"[{server.name}] {line}")
            else:
                await _broadcast(f"[{server.name}] Connection failed: {msg}")
        await _broadcast_server_status()
        return results

    @app.post("/api/disconnect")
    async def disconnect_servers(body: dict):
        if body.get("all"):
            pool.disconnect_all()
            await _broadcast("All connections closed")
            await _broadcast_server_status()
            return {"status": "all disconnected"}

        for name in body.get("servers", []):
            pool.disconnect(name)
            await _broadcast(f"[{name}] Disconnected")
        await _broadcast_server_status()
        return {"status": "ok"}

    @app.post("/api/dry-run")
    async def dry_run(body: dict):
        """Preview tasks without executing. Same body as /api/run."""
        server_names = body.get("servers", [])
        task_names = body.get("tasks")
        servers = config.filter_servers(server_names)
        if not servers:
            return {"error": "no servers matched"}
        if task_names:
            tasks = [t for t in config.tasks if t.name in task_names]
        else:
            tasks = config.tasks
        if not tasks:
            return {"error": "no tasks found"}
        return {
            "servers": [s.name for s in servers],
            "tasks": [{
                "name": t.name,
                "type": t.type,
                "detail": t.command if t.type == "command" else (
                    f"{t.src} \u2192 {t.dest}" if t.type == "upload" else
                    f"{t.src} {t.args}".strip() if t.type == "script" else t.name),
                "modifying": is_modifying_command(t.command) if t.type == "command" and t.command else False,
            } for t in tasks],
        }

    @app.post("/api/run")
    async def run_tasks(body: dict):
        """Run tasks. Body: {"servers": [...], "tasks": [...] (optional)}"""
        server_names = body.get("servers", [])
        task_names = body.get("tasks")

        servers = config.filter_servers(server_names)
        if not servers:
            return {"error": "no servers matched"}

        if task_names:
            tasks = [t for t in config.tasks if t.name in task_names]
        else:
            tasks = config.tasks

        if not tasks:
            return {"error": "no tasks found"}

        # Run in background thread to not block
        def _run():
            _cancel_event.clear()
            task_names = [t.name for t in tasks]
            server_names = [s.name for s in servers]
            _task_status.update({
                "running": True,
                "cancelling": False,
                "servers": {sn: {tn: {"status": "pending"} for tn in task_names} for sn in server_names},
                "server_names": server_names,
                "task_names": task_names,
                "current_server": "",
            })
            _broadcast_from_thread(json.dumps({"type": "progress", **_task_status}))

            executor.run_all_tasks(servers, tasks, serial=True,
                                   progress_callback=_progress_callback,
                                   cancel_event=_cancel_event)
            # Mark any remaining pending tasks as cancelled
            if _cancel_event.is_set():
                for sn in server_names:
                    for tn in task_names:
                        info = _task_status["servers"].get(sn, {}).get(tn, {})
                        if info.get("status") == "pending":
                            _task_status["servers"].setdefault(sn, {})[tn] = {"status": "cancelled"}
            done = sum(1 for sn in server_names for tn in task_names
                       if _task_status["servers"].get(sn, {}).get(tn, {}).get("status") == "done")
            failed = sum(1 for sn in server_names for tn in task_names
                         if _task_status["servers"].get(sn, {}).get(tn, {}).get("status") == "done"
                         and not _task_status["servers"][sn][tn].get("success"))
            cancelled = sum(1 for sn in server_names for tn in task_names
                            if _task_status["servers"].get(sn, {}).get(tn, {}).get("status") == "cancelled")
            _task_status["running"] = False
            _broadcast_from_thread(json.dumps({"type": "progress", **_task_status}))
            if _cancel_event.is_set():
                _broadcast_from_thread(f"⚠ Cancelled — {done} ran, {cancelled} skipped"
                                       + (f", {failed} failed" if failed else ""))
            elif failed:
                _broadcast_from_thread(f"✗ Done — {failed}/{done} failed")
            elif len(server_names) > 1:
                _broadcast_from_thread(f"✓ Done — {done}/{done} passed")

        # Register WS broadcast as logger callback
        logger.register_ws_callback(_broadcast_from_thread)
        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

        return {"status": "started", "servers": [s.name for s in servers],
                "tasks": [t.name for t in tasks]}

    @app.post("/api/run-command")
    async def run_adhoc_command(body: dict):
        """Run ad-hoc command. Body: {"servers": [...], "command": "...", "silent": false}"""
        server_names = body.get("servers", [])
        command = body.get("command", "").strip()
        silent = body.get("silent", False)
        if not command:
            return {"error": "no command provided"}
        interactive_err = check_interactive_command(command)
        if interactive_err:
            return {"error": interactive_err}

        servers = config.filter_servers(server_names)
        if not servers:
            return {"error": "no servers matched"}

        # Save to history (deduplicate: remove if exists, then append)
        if command in cmd_history:
            cmd_history.remove(command)
        cmd_history.append(command)
        _save_history()

        def _run():
            failed = 0
            total = len(servers)
            for server in servers:
                session = pool.get_session(server.name)
                if not session:
                    if not silent:
                        logger.error(f"[{server.name}] Not connected")
                    failed += 1
                    continue

                log_path = logger.create_exec_log(server.name, "adhoc")
                ts = datetime.now().strftime("%H:%M:%S")
                if not silent:
                    logger.info(f"[{server.name}] $ {command}")
                logger.write_exec_log(log_path, f"\n[{ts}] $ {command}\n")
                lines, exit_code = _exhaust_generator(session.exec_command(command))
                exit_code = exit_code or 0
                for line in lines:
                    ts = datetime.now().strftime("%H:%M:%S")
                    if not silent:
                        logger.info(f"[{server.name}] {line}")
                    logger.write_exec_log(log_path, f"[{ts}] {line}\n")
                if exit_code != 0:
                    if not silent:
                        logger.error(f"[{server.name}] Command failed (exit={exit_code})")
                    failed += 1

            if not silent and total > 1:
                if failed:
                    _broadcast_from_thread(f"✗ Done — {failed}/{total} failed")
                else:
                    _broadcast_from_thread(f"✓ Done — {total}/{total} passed")

        logger.register_ws_callback(_broadcast_from_thread)
        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

        return {"status": "started"}

    @app.post("/api/run-command-compare")
    async def run_command_compare(body: dict):
        """Run command and return per-server results for comparison.
        Body: {"servers": [...], "command": "..."}
        Returns: {"results": {"server1": {"lines": [...], "exit_code": 0}, ...}}
        """
        server_names = body.get("servers", [])
        command = body.get("command", "").strip()
        if not command:
            return {"error": "no command provided"}
        interactive_err = check_interactive_command(command)
        if interactive_err:
            return {"error": interactive_err}
        servers = config.filter_servers(server_names)
        if not servers:
            return {"error": "no servers matched"}

        import concurrent.futures

        def _run_one(server):
            session = pool.get_session(server.name)
            if not session:
                return server.name, {"lines": ["Not connected"], "exit_code": -1}
            try:
                lines, exit_code = _exhaust_generator(session.exec_command(command))
                return server.name, {"lines": lines, "exit_code": exit_code or 0}
            except Exception as e:
                return server.name, {"lines": [str(e)], "exit_code": -1}

        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(servers)) as ex:
            futures = [ex.submit(_run_one, s) for s in servers]
            for f in concurrent.futures.as_completed(futures):
                name, data = f.result()
                results[name] = data

        return {"results": results}

    @app.post("/api/read-file")
    async def read_remote_file(body: dict):
        """Read a file from a remote server. Body: {"server": "name", "path": "/abs/path"}"""
        server_name = body.get("server", "")
        file_path = body.get("path", "").strip()
        if not file_path or not file_path.startswith("/"):
            return {"error": "path must be an absolute path"}
        session = pool.get_session(server_name)
        if not session:
            return {"error": f"Server '{server_name}' not connected"}
        try:
            # Check if path is a directory
            dir_lines, _ = _exhaust_generator(
                session.exec_command(f"test -d {_shell_quote(file_path)} && echo DIR", timeout=10)
            )
            if dir_lines and "DIR" in dir_lines[0]:
                return {"error": "Is a directory — cannot read as text"}
            # Check if file is binary
            type_lines, _ = _exhaust_generator(
                session.exec_command(f"file -b --mime-encoding {_shell_quote(file_path)}", timeout=10)
            )
            encoding = (type_lines[0] if type_lines else "").strip().lower()
            if encoding == "binary":
                return {"error": "Binary file — cannot read as text"}

            lines, exit_code = _exhaust_generator(
                session.exec_command(f"cat {_shell_quote(file_path)}", timeout=30)
            )
            if exit_code and exit_code != 0:
                err_text = "\n".join(lines) if lines else f"exit code {exit_code}"
                return {"error": err_text}
            return {"content": "\n".join(lines), "path": file_path, "server": server_name}
        except Exception as e:
            return {"error": str(e)}

    @app.post("/api/write-file")
    async def write_remote_file(body: dict):
        """Write content to a remote file. Body: {"server": "name", "path": "/abs/path", "content": "..."}"""
        server_name = body.get("server", "")
        file_path = body.get("path", "").strip()
        content = body.get("content", "")
        if not file_path or not file_path.startswith("/"):
            return {"error": "path must be an absolute path"}
        session = pool.get_session(server_name)
        if not session:
            return {"error": f"Server '{server_name}' not connected"}
        try:
            # Write via base64 to handle special characters safely
            import base64
            b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")
            cmd = f"echo {_shell_quote(b64)} | base64 -d > {_shell_quote(file_path)}"
            lines, exit_code = _exhaust_generator(
                session.exec_command(cmd, timeout=30)
            )
            if exit_code and exit_code != 0:
                err_text = "\n".join(lines) if lines else f"exit code {exit_code}"
                return {"error": err_text}
            await _broadcast(f"[{server_name}] File saved: {file_path}")
            return {"status": "ok"}
        except Exception as e:
            return {"error": str(e)}

    @app.post("/api/download-file")
    async def download_remote_file(body: dict):
        """Download a file from remote server(s). Body: {"servers": [...], "path": "/abs/path"}
        Returns: {"files": {"server1": {"content": "...", "size": 123}, ...}}
        """
        server_names = body.get("servers", [])
        file_path = body.get("path", "").strip()
        if not file_path or not file_path.startswith("/"):
            return {"error": "path must be an absolute path"}
        servers = config.filter_servers(server_names)
        if not servers:
            return {"error": "no servers matched"}

        import concurrent.futures

        def _dl_one(server):
            session = pool.get_session(server.name)
            if not session:
                return server.name, {"error": "Not connected"}
            try:
                # Use base64 to safely transfer binary-compatible content
                cmd = f"base64 {_shell_quote(file_path)} 2>/dev/null"
                lines, exit_code = _exhaust_generator(session.exec_command(cmd, timeout=30))
                if exit_code and exit_code != 0:
                    # Fall back to cat for error message
                    lines2, _ = _exhaust_generator(
                        session.exec_command(f"cat {_shell_quote(file_path)} 2>&1", timeout=10))
                    return server.name, {"error": "\n".join(lines2) if lines2 else f"exit code {exit_code}"}
                import base64
                raw = "".join(lines)
                raw_bytes = base64.b64decode(raw)
                try:
                    content = raw_bytes.decode("utf-8")
                    return server.name, {"content": content, "size": len(raw_bytes)}
                except UnicodeDecodeError:
                    # Binary file — return base64 for frontend to decode
                    return server.name, {"b64": raw, "size": len(raw_bytes), "binary": True}
            except Exception as e:
                return server.name, {"error": str(e)}

        files = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(servers)) as ex:
            futures = [ex.submit(_dl_one, s) for s in servers]
            for f in concurrent.futures.as_completed(futures):
                name, data = f.result()
                files[name] = data

        return {"files": files}

    @app.post("/api/delete-remote-file")
    async def delete_remote_file(body: dict):
        """Delete a file on remote server(s). Body: {"servers": [...], "path": "/abs/path"}"""
        server_names = body.get("servers", [])
        file_path = body.get("path", "").strip()
        if not file_path or not file_path.startswith("/"):
            return {"error": "path must be an absolute path"}
        servers = config.filter_servers(server_names)
        if not servers:
            return {"error": "no servers matched"}

        results = {}
        for s in servers:
            session = pool.get_session(s.name)
            if not session:
                results[s.name] = {"error": "Not connected"}
                continue
            try:
                lines, exit_code = _exhaust_generator(
                    session.exec_command(f"rm -f {_shell_quote(file_path)} 2>&1", timeout=15)
                )
                if exit_code and exit_code != 0:
                    results[s.name] = {"error": "\n".join(lines) if lines else f"exit code {exit_code}"}
                else:
                    results[s.name] = {"status": "ok"}
                    await _broadcast(f"[{s.name}] Deleted: {file_path}")
            except Exception as e:
                results[s.name] = {"error": str(e)}
        return {"results": results}

    @app.post("/api/upload")
    async def upload_file_to_servers(
        file: UploadFile = File(...),
        dest: str = Form(...),
        servers: str = Form(""),
        mode: str = Form(""),
    ):
        """Upload file via multipart form. dest must be an absolute file path."""
        if not dest.startswith("/"):
            return {"error": "dest must be an absolute path (e.g. /opt/app/file.txt)"}
        if dest.endswith("/") or dest.endswith("\\"):
            return {"error": "dest must be a file path, not a directory"}
        server_names = [s.strip() for s in servers.split(",") if s.strip()]
        targets = config.filter_servers(server_names if server_names else None)

        if not targets:
            return {"error": "no servers matched"}

        # Sanitize filename to prevent path traversal
        safe_filename = Path(file.filename).name
        if not safe_filename:
            return {"error": "invalid filename"}

        # Save uploaded file to temp location, with guaranteed cleanup
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_file = Path(tmp_dir) / safe_filename
            with open(tmp_file, "wb") as f:
                shutil.copyfileobj(file.file, f)

            results = {}
            for server in targets:
                session = pool.get_session(server.name)
                if not session:
                    results[server.name] = "not connected"
                    continue
                try:
                    session.upload_file(str(tmp_file), dest, mode or None)
                    results[server.name] = "uploaded"
                    await _broadcast(
                        f"[{server.name}] Uploaded: {safe_filename} -> {dest}"
                    )
                except Exception as e:
                    results[server.name] = f"error: {e}"
                    await _broadcast(f"[{server.name}] Upload failed: {e}")

        # Save to upload history
        entry = {"filename": safe_filename, "dest": dest}
        # Deduplicate
        for existing in list(upload_history):
            if existing["filename"] == safe_filename and existing["dest"] == dest:
                upload_history.remove(existing)
                break
        upload_history.append(entry)
        _save_history()

        return results

    # --- WebSocket ---
    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket):
        await ws.accept()
        # Send history to newly connected client
        for line in output_history:
            try:
                await ws.send_text(line)
            except Exception:  # pragma: no cover — client disconnected during history replay
                return
        ws_clients.add(ws)
        try:
            await ws.send_text(json.dumps({"type": "server_status", "servers": _get_server_status()}))
        except Exception:  # pragma: no cover
            pass
        try:
            while True:
                await ws.receive_text()  # Keep connection alive
        except WebSocketDisconnect:
            ws_clients.discard(ws)

    async def _broadcast(message: str):
        output_history.append(message)
        dead = set()
        for ws in ws_clients:
            try:
                await ws.send_text(message)
            except Exception:  # pragma: no cover — dead client cleanup
                dead.add(ws)
        ws_clients.difference_update(dead)

    return app


def create_app_from_env() -> FastAPI:
    """Factory for uvicorn reload mode — reads config from environment variables."""
    import os

    config_path = os.environ["_SSH_OPS_CONFIG"]
    log_dir = os.environ.get("_SSH_OPS_LOG_DIR", "")

    config = AppConfig(config_path)
    logger = ExecLogger(Path(log_dir) if log_dir else config.log_dir)
    return create_app(config, logger)
