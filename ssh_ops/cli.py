"""CLI entry point — command-line interface for SSH Ops Tool."""

import argparse
import sys
from pathlib import Path

from .config import AppConfig, is_modifying_command
from .executor import ConnectionPool, TaskExecutor, _exhaust_generator
from .logger import ExecLogger


def main():
    parser = argparse.ArgumentParser(
        prog="ssh-ops",
        description="Lightweight multi-server SSH tool with real-time web UI"
    )
    parser.add_argument(
        "-c", "--config",
        help="config file path (default: config/default.yml)"
    )
    parser.add_argument(
        "--reload", action="store_true",
        help="auto-reload on code changes (dev mode, serve only)"
    )

    sub = parser.add_subparsers(dest="command")

    # --- list ---
    list_parser = sub.add_parser("list", help="list servers or tasks")
    list_parser.add_argument("target", choices=["servers", "tasks"])

    # --- run ---
    run_parser = sub.add_parser("run", help="run all tasks (or specific task)")
    run_parser.add_argument("--task", help="run specific task by name")
    run_parser.add_argument("--server", help="comma-separated server names")
    run_parser.add_argument("--group", help="server group name")
    run_parser.add_argument(
        "--parallel", action="store_true",
        help="run on all servers in parallel (default: serial)"
    )
    run_parser.add_argument(
        "--dry-run", action="store_true",
        help="preview tasks without executing"
    )

    # --- exec ---
    exec_parser = sub.add_parser("exec", help="run ad-hoc command")
    exec_parser.add_argument("cmd", help="command to run")
    exec_parser.add_argument("--server", help="comma-separated server names")
    exec_parser.add_argument("--group", help="server group name")

    # --- upload ---
    upload_parser = sub.add_parser("upload", help="upload file to servers")
    upload_parser.add_argument("src", help="local file path")
    upload_parser.add_argument("dest", help="remote destination path")
    upload_parser.add_argument("--server", help="comma-separated server names")
    upload_parser.add_argument("--group", help="server group name")
    upload_parser.add_argument("--mode", help="file permission (e.g. 0755)")

    # --- serve ---
    serve_parser = sub.add_parser("serve", help="start web server")
    serve_parser.add_argument("--host", help="bind address")
    serve_parser.add_argument("--port", type=int, help="bind port")
    serve_parser.add_argument("--reload", action="store_true", help="auto-reload on code changes (dev mode)")

    args = parser.parse_args()

    if not args.command:
        args.command = "serve"
        args.host = None
        args.port = None

    config_path = Path(args.config).resolve() if args.config else AppConfig.default_config_path()

    if not config_path.exists():
        if args.command == "serve":
            # Auto-create an empty config so the web UI can start
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text("servers: []\ntasks: []\n", encoding="utf-8")
            print(f"Created empty config: {config_path}")
        else:
            print(f"Error: config file not found: {config_path}")
            print("Hint: use 'ssh-ops serve' to start the web UI and create configs interactively.")
            sys.exit(1)

    config = AppConfig(str(config_path))
    AppConfig._save_last_config(config.config_path)
    logger = ExecLogger(config.log_dir)

    if args.command == "list":
        _do_list(config, args)
    elif args.command == "run":
        _do_run(config, logger, args)
    elif args.command == "exec":
        _do_exec(config, logger, args)
    elif args.command == "upload":
        _do_upload(config, logger, args)
    elif args.command == "serve":
        _do_serve(config, logger, args)


def _do_list(config: AppConfig, args):
    if args.target == "servers":
        print(f"{'Name':<20} {'Host':<20} {'Port':<6} {'User':<15} {'Groups'}")
        print("-" * 80)
        for s in config.servers:
            groups = ",".join(s.groups) if s.groups else "-"
            print(f"{s.name:<20} {s.host:<20} {s.port:<6} {s.username:<15} {groups}")
    elif args.target == "tasks":
        print(f"{'Name':<25} {'Type':<10} {'Detail'}")
        print("-" * 70)
        for t in config.tasks:
            if t.type == "upload":
                detail = f"{t.src} -> {t.dest}"
            elif t.type == "command":
                detail = t.command
            elif t.type == "script":
                detail = f"{t.src} {t.args}".strip()
            else:  # pragma: no cover — all known types handled above
                detail = ""
            print(f"{t.name:<25} {t.type:<10} {detail}")


def _do_run(config: AppConfig, logger: ExecLogger, args):
    server_names = args.server.split(",") if args.server else None
    servers = config.filter_servers(server_names, args.group)

    if not servers:
        logger.error("No servers matched")
        sys.exit(1)

    if args.task:
        task = config.get_task(args.task)
        if not task:
            logger.error(f"Task not found: {args.task}")
            sys.exit(1)
        tasks = [task]
    else:
        tasks = config.tasks

    if not tasks:
        logger.error("No tasks defined")
        sys.exit(1)

    if args.dry_run:
        print(f"\n{'═' * 60}")
        print(f"  DRY RUN PREVIEW")
        print(f"{'═' * 60}")
        print(f"  Servers ({len(servers)}):")
        for i, s in enumerate(servers, 1):
            print(f"    {i}. {s.name}")
        print(f"  Tasks ({len(tasks)}):")
        for i, t in enumerate(tasks, 1):
            if t.type == "command":
                detail = t.command
                mod = " ⚠ modifying" if is_modifying_command(t.command) else ""
            elif t.type == "upload":
                detail = f"{t.src} → {t.dest}"
                mod = ""
            elif t.type == "script":
                detail = f"{t.src} {t.args}".strip()
                mod = ""
            else:
                detail = t.name
                mod = ""
            print(f"    {i}. [{t.type:<7}] {t.name}: {detail}{mod}")
        print(f"{'═' * 60}\n")
        return

    pool = ConnectionPool(config.keep_alive)
    executor = TaskExecutor(pool, logger)

    try:
        for server in servers:
            logger.info(f"Connecting to {server.name} ({server.host})...")
            try:
                pool.connect(server)
                logger.info(f"Connected to {server.name}")
            except Exception as e:
                logger.error(f"Failed to connect to {server.name}: {e}")
                sys.exit(1)

        serial = not args.parallel
        results = executor.run_all_tasks(servers, tasks, serial=serial)

        all_ok = all(
            r["success"]
            for task_results in results.values()
            for r in task_results
        )
        sys.exit(0 if all_ok else 1)
    finally:
        pool.disconnect_all()


def _do_exec(config: AppConfig, logger: ExecLogger, args):
    server_names = args.server.split(",") if args.server else None
    servers = config.filter_servers(server_names, args.group)

    if not servers:
        logger.error("No servers matched")
        sys.exit(1)

    pool = ConnectionPool(config.keep_alive)
    any_failed = False
    try:
        for server in servers:
            pool.connect(server)
            session = pool.get_session(server.name)
            if not session:
                logger.error(f"Failed to connect to {server.name}")
                any_failed = True
                continue

            log_path = logger.create_exec_log(server.name, "adhoc")
            logger.info(f"[{server.name}] $ {args.cmd}")
            lines, exit_code = _exhaust_generator(session.exec_command(args.cmd))
            exit_code = exit_code or 0
            for line in lines:
                logger.info(f"[{server.name}] {line}")
                logger.write_exec_log(log_path, f"{line}\n")
            if exit_code != 0:
                logger.error(f"[{server.name}] Command failed (exit={exit_code})")
                any_failed = True
    finally:
        pool.disconnect_all()
    if any_failed:
        sys.exit(1)


def _do_upload(config: AppConfig, logger: ExecLogger, args):
    server_names = args.server.split(",") if args.server else None
    servers = config.filter_servers(server_names, args.group)

    if not servers:
        logger.error("No servers matched")
        sys.exit(1)

    src = str(Path(args.src).resolve())
    if not Path(src).exists():
        logger.error(f"Local file not found: {src}")
        sys.exit(1)

    pool = ConnectionPool(config.keep_alive)
    try:
        for server in servers:
            pool.connect(server)
            session = pool.get_session(server.name)
            if not session:
                logger.error(f"Failed to connect to {server.name}")
                continue

            session.upload_file(src, args.dest, args.mode)
            logger.info(f"[{server.name}] Uploaded: {src} -> {args.dest}")
    finally:
        pool.disconnect_all()


def _do_serve(config: AppConfig, logger: ExecLogger, args):
    from .server import create_app
    import uvicorn

    host = args.host or config.web_host
    port = args.port or config.web_port

    logger.info(f"Starting web server at http://{host}:{port}")
    if args.reload:
        # reload mode: use uvicorn CLI directly for reliable process management
        import os, subprocess
        env = os.environ.copy()
        env["_SSH_OPS_CONFIG"] = str(config.config_path)
        env["_SSH_OPS_LOG_DIR"] = str(logger.log_dir)
        cmd = [
            sys.executable, "-m", "uvicorn",
            "ssh_ops.server:create_app_from_env",
            "--host", host, "--port", str(port),
            "--reload",
            "--reload-dir", str(Path(__file__).parent),
            "--reload-include", "*.py",
            "--reload-include", "*.html",
        ]
        subprocess.run(cmd, env=env)
    else:
        app = create_app(config, logger)
        uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":  # pragma: no cover
    main()
