#!/usr/bin/env python3
"""Fake SSH server for local testing. Supports command execution and SFTP upload."""

import logging
import os
import socket
import sys
import tempfile
import threading
from pathlib import Path

import paramiko

logging.basicConfig(level=logging.INFO, format="[FakeSSH] %(message)s")
log = logging.getLogger("fake_ssh")

# Generate a temporary host key
HOST_KEY = paramiko.RSAKey.generate(2048)

# Fake filesystem root for uploads
UPLOAD_DIR = Path(tempfile.mkdtemp(prefix="ssh_ops_test_"))
log.info(f"Upload directory: {UPLOAD_DIR}")


class FakeServer(paramiko.ServerInterface):
    def __init__(self):
        self.event = threading.Event()
        self._exec_queue = []

    def check_channel_request(self, kind, chanid):
        if kind == "session":
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_auth_password(self, username, password):
        # Accept any username/password
        log.info(f"Auth: {username} (accepted)")
        return paramiko.AUTH_SUCCESSFUL

    def check_auth_publickey(self, username, key):
        return paramiko.AUTH_SUCCESSFUL

    def get_allowed_auths(self, username):
        return "password,publickey"

    def check_channel_exec_request(self, channel, command):
        self._exec_queue.append((channel, command.decode("utf-8")))
        self.event.set()
        return True

    def check_channel_shell_request(self, channel):
        self.event.set()
        return True


class FakeSFTPHandle(paramiko.SFTPHandle):
    def __init__(self, path, flags=0):
        super().__init__(flags)
        self.path = path
        self._file = open(path, "wb" if flags else "rb")

    def write(self, offset, data):
        self._file.seek(offset)
        self._file.write(data)
        return paramiko.SFTP_OK

    def read(self, offset, length):
        self._file.seek(offset)
        return self._file.read(length)

    def stat(self):
        return paramiko.SFTPAttributes.from_stat(os.stat(self.path))

    def close(self):
        self._file.close()
        super().close()


class FakeSFTPServer(paramiko.SFTPServerInterface):
    def __init__(self, server, *args, **kwargs):
        super().__init__(server, *args, **kwargs)

    def _realpath(self, path):
        # Map remote paths to local upload dir
        return str(UPLOAD_DIR / path.lstrip("/"))

    def open(self, path, flags, attr):
        realpath = self._realpath(path)
        Path(realpath).parent.mkdir(parents=True, exist_ok=True)
        log.info(f"SFTP upload: {path} -> {realpath}")
        return FakeSFTPHandle(realpath, flags)

    def stat(self, path):
        realpath = self._realpath(path)
        if os.path.exists(realpath):
            return paramiko.SFTPAttributes.from_stat(os.stat(realpath))
        return paramiko.SFTP_NO_SUCH_FILE

    def lstat(self, path):
        return self.stat(path)

    def chattr(self, path, attr):
        realpath = self._realpath(path)
        if attr.st_mode:
            os.chmod(realpath, attr.st_mode)
        return paramiko.SFTP_OK

    def list_folder(self, path):
        realpath = self._realpath(path)
        if not os.path.isdir(realpath):
            return paramiko.SFTP_NO_SUCH_FILE
        out = []
        for name in os.listdir(realpath):
            full = os.path.join(realpath, name)
            attr = paramiko.SFTPAttributes.from_stat(os.stat(full))
            attr.filename = name
            out.append(attr)
        return out


def _execute_on_channel(channel, cmd):
    """Execute a fake command and send output on the channel."""
    exit_code = 0
    try:
        if cmd.startswith("echo "):
            output = cmd[5:].strip().strip('"').strip("'")
            channel.sendall(f"{output}\n".encode())
        elif cmd == "df -h":
            channel.sendall(
                b"Filesystem      Size  Used Avail Use% Mounted on\n"
                b"/dev/sda1        50G   20G   28G  42% /\n"
            )
        elif cmd == "whoami":
            channel.sendall(b"testuser\n")
        elif cmd == "hostname":
            channel.sendall(b"fake-server\n")
        elif cmd == "uname -a":
            channel.sendall(b"Linux fake-server 5.15.0 #1 SMP x86_64 GNU/Linux\n")
        elif cmd == "uptime":
            channel.sendall(b" 14:30:00 up 42 days, 3:15, 1 user, load average: 0.1, 0.2, 0.1\n")
        elif cmd.startswith("ls"):
            files = list(UPLOAD_DIR.rglob("*"))
            if files:
                for f in files:
                    channel.sendall(f"{f.relative_to(UPLOAD_DIR)}\n".encode())
            else:
                channel.sendall(b"(empty)\n")
        elif cmd.startswith("cat "):
            filepath = UPLOAD_DIR / cmd[4:].strip().lstrip("/")
            if filepath.exists():
                channel.sendall(filepath.read_bytes())
                channel.sendall(b"\n")
            else:
                channel.sendall_stderr(f"cat: {cmd[4:]}: No such file\n".encode())
                exit_code = 1
        elif cmd.startswith("rm "):
            pass
        else:
            channel.sendall(f"[fake] executed: {cmd}\n".encode())
            channel.sendall(f"[fake] exit code: 0\n".encode())
    except Exception as e:
        log.info(f"Channel error: {e}")

    try:
        channel.send_exit_status(exit_code)
        channel.shutdown_write()
        channel.close()
    except Exception:
        pass


def handle_client(client_socket):
    transport = paramiko.Transport(client_socket)
    transport.add_server_key(HOST_KEY)
    transport.set_subsystem_handler("sftp", paramiko.SFTPServer, FakeSFTPServer)

    server = FakeServer()
    transport.start_server(server=server)

    # Process exec requests as they come in
    try:
        while transport.is_active():
            # Wait for an exec request
            server.event.wait(timeout=1)
            if not transport.is_active():
                break
            # Process any queued commands
            while server._exec_queue:
                channel, cmd = server._exec_queue.pop(0)
                log.info(f"Exec: {cmd}")
                threading.Thread(
                    target=_execute_on_channel,
                    args=(channel, cmd),
                    daemon=True,
                ).start()
            server.event.clear()
    except Exception:
        pass
    finally:
        transport.close()


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 2222
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", port))
    sock.listen(5)

    log.info(f"Fake SSH server listening on 127.0.0.1:{port}")
    log.info(f"Accepts any username/password")
    log.info(f"Ctrl+C to stop\n")

    try:
        while True:
            client, addr = sock.accept()
            log.info(f"Connection from {addr}")
            threading.Thread(target=handle_client, args=(client,), daemon=True).start()
    except KeyboardInterrupt:
        log.info("Shutting down")
    finally:
        sock.close()


if __name__ == "__main__":
    main()
