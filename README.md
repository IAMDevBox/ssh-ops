# SSH Ops Tool

A lightweight multi-server SSH tool with a real-time web UI. Run commands, upload files, and execute tasks across servers — configured in simple YAML.

## Setup

```bash
pip install -e .
# or: pip install -r requirements.txt
```

## Quick Start

```bash
# Start the web UI (default when no command given)
ssh-ops

# Or run tasks from a config file
ssh-ops -c config/test.yml run
```

For real servers, edit `config/default.yml` with your hosts and tasks, then run without `-c`.
You can also create and manage configs from the web UI.

## Configuration

Default config file: `config/default.yml`. Use `-c` to specify a different one.
When running `ssh-ops serve` without a config file, an empty one is created automatically.

### Minimal example

```yaml
servers:
  - host: 192.168.1.10
    username: admin
    password: $SSH_PASSWORD

  - host: 192.168.1.11
    username: admin
    password: $SSH_PASSWORD

tasks:
  # src and dest must be absolute file paths, NOT directories
  - src: /home/admin/files/app.conf
    dest: /opt/app/config/app.conf

  - src: /home/admin/scripts/deploy.sh
    dest: /opt/app/deploy.sh
    mode: 0755

  - command: /opt/app/deploy.sh

  - command: echo "done"
```

### Full example (all optional fields)

```yaml
servers:
  - host: 192.168.1.10
    username: admin
    password: $SSH_PASSWORD        # env var reference
    # key_file: ~/.ssh/id_rsa    # Windows: C:\Users\you\.ssh\id_rsa
    # port: 22
    # groups: [web, dev]

tasks:
  - name: deploy-config
    type: upload
    src: /home/admin/files/app.conf  # absolute file path, NOT directory
    dest: /opt/app/config/app.conf   # absolute file path, NOT directory

  - name: run-deploy
    type: command
    command: /opt/app/deploy.sh
    timeout: 300
    env:
      ENV: production

settings:                         # entirely optional section
  log_dir: ./logs
  web_host: 127.0.0.1
  web_port: 8080
```

### Config Notes

- **Servers**: only `host` is required. Username defaults to OS user. SSH key auto-detected from `~/.ssh/`.
- **Server shorthand**: `"user@host"` or `"user@host:port"` instead of multi-line YAML.
- **Tasks**: `type` auto-detected — `src` + `dest` = upload, `command` = command. `name` auto-generated if omitted.
- **Task types**: `upload` (send file), `command` (run remote command), `script` (upload + execute + cleanup).
- **Paths**: both `src` and `dest` must be **absolute file paths** (e.g. `/home/admin/files/app.conf`), NOT directories. The web UI multi-file upload is the only exception — it accepts a directory path ending with `/`.
- **Passwords**: support `$ENV_VAR` or `${ENV_VAR}` references.
- **Settings**: all have defaults, the entire section can be omitted.
- **Interactive commands NOT allowed**: `top`, `vim`, `less`, `ssh`, `watch`, `tail -f`, `ping` (without `-c`), etc. Use non-interactive alternatives (e.g. `top -b -n 1`).
- Tasks execute in defined order on each server sequentially.

### Optional Settings

Add a `settings` section to your config file to override defaults:

```yaml
settings:
  log_dir: ./logs              # log output directory (default: ./logs)
  default_timeout: 120         # command timeout in seconds (default: 120)
  keep_alive: 60               # SSH keepalive interval in seconds (default: 60)
  web_host: 127.0.0.1          # web server bind address (default: 127.0.0.1)
  web_port: 8080               # web server port (default: 8080)
```

All fields are optional. Only include the ones you want to change.

## CLI Usage

Install with `pip install -e .` to get the `ssh-ops` command. Also works as `python ssh-ops.py` or `python -m ssh_ops`.
Running `ssh-ops` without a command defaults to `serve` (start web UI).

All CLI commands read `config/default.yml` by default. Use `-c` to specify a different config file:

```bash
ssh-ops -c config/docker-test.yml list servers
ssh-ops -c config/prod.yml run
```

### list — show config

```bash
ssh-ops list servers
ssh-ops list tasks
```

### run — execute tasks from config

```bash
# Run all tasks on all servers (one server at a time, tasks in order)
ssh-ops run

# Run all tasks on specific servers only
ssh-ops run --server 192.168.1.10,192.168.1.11

# Run all tasks on servers in a group
ssh-ops run --group dev

# Run one specific task on all servers
ssh-ops run --task upload-1

# Run one specific task on one specific server
ssh-ops run --task upload-1 --server 192.168.1.10

# Run all tasks on all servers in parallel (all servers at once)
ssh-ops run --parallel

# Preview tasks without executing (dry run)
ssh-ops run --dry-run
ssh-ops run --task upload-1 --server 192.168.1.10 --dry-run
```

### exec — run ad-hoc command (not in config)

```bash
# Run a command on one server
ssh-ops exec "df -h" --server 192.168.1.10

# Run a command on multiple servers
ssh-ops exec "uptime" --server 192.168.1.10,192.168.1.11

# Run a command on all servers in a group
ssh-ops exec "whoami" --group dev
```

### upload — send a local file to remote server (not in config)

Usage: `ssh-ops upload <local_path> <remote_path> [options]`

- 1st argument: local file path (source)
- 2nd argument: remote **absolute file path** (destination, not directory)

```bash
# Upload a file to one server
ssh-ops upload ./file.txt /opt/app/file.txt --server 192.168.1.10

# Upload a script with execute permission
ssh-ops upload ./deploy.sh /opt/app/deploy.sh --server 192.168.1.10 --mode 0755

# Upload to all servers in a group
ssh-ops upload ./app.conf /opt/app/app.conf --group dev
```

### serve — start web UI

See [Web Mode](#web-mode) below.

### help

```bash
ssh-ops -h           # general help
ssh-ops run -h       # help for run command
ssh-ops exec -h      # help for exec command
```

## Web Mode

```bash
ssh-ops                                    # default command, auto-creates config if needed
ssh-ops serve --host 0.0.0.0 --port 9090   # custom bind address
```

Open `http://127.0.0.1:8080` in a browser.

### Web UI Features

- **Config management** — create, rename, delete, download, edit YAML, and switch between configs from the header dropdown
- **Config editor** — edit config YAML directly in the browser with syntax highlighting, validation, atomic save, and auto-backup (`.bak`)
- **Servers** — select servers, connect/disconnect (with confirmation), copy selected hosts to clipboard; count badge shows selected/total
- **Tasks** — check tasks and click Run; add command or upload tasks via menu; drag to reorder; view raw YAML; count badge shows selected/total
- **Task progress** — real-time status icons on each task (○ pending, ◎ running, ✓ done, ✗ failed) and progress bar with server info
- **Ad-hoc commands** — enter commands in the input box, run on selected servers; up/down arrow keys cycle through history
- **Scheduled commands** — run a command periodically on selected servers (configurable interval, per-config persistence)
- **File upload** — upload files via file picker (with optional exec mode)
- **Real-time output** — WebSocket streaming, per-server log tabs
- **Output styling** — input/output/info lines visually distinguished; per-block copy buttons; timestamp toggle (T button, persisted)
- **History** — command and upload history persisted per config, survives restarts
- **Safety** — modifying commands highlighted with amber warning; interactive commands blocked
- **Output persistence** — output survives page refreshes

## Docker Test Environment

A Docker Compose setup provides 3 real SSH nodes (Rocky Linux 9) for testing.

### Start / Stop

```bash
docker/start.sh     # build image and start 3 nodes
docker/stop.sh      # stop and remove containers
```

### Test Nodes

| Node  | Port | Username   | Password | Notes         |
|-------|------|------------|----------|---------------|
| node1 | 2201 | testuser   | test123  |               |
| node2 | 2202 | testuser   | test123  |               |
| node3 | 2203 | testuser   | test123  |               |

An `admin` user (password: `admin123`, sudo) is also available on all nodes.

### Usage

Use `config/docker-test.yml` which is pre-configured for the Docker nodes:

```bash
ssh-ops -c config/docker-test.yml
```

Resource usage is minimal (~5MB RAM per node, near-zero CPU when idle).

## Logs

```
logs/
├── session.log                              # all output
├── 192.168.1.10/
│   └── 2026-03-07.log                       # one file per server per day
└── 192.168.1.11/
    └── 2026-03-07.log
```
