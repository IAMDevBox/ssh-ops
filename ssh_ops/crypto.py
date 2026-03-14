"""Password encryption for YAML config files.

Passwords are stored as ENC(base64-ciphertext) in YAML.
A master password derives a Fernet key via PBKDF2-HMAC-SHA256.
The salt is stored alongside the config as .salt file.
"""

import base64
import os
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

_ENC_PREFIX = "ENC("
_ENC_SUFFIX = ")"


def _derive_key(master_password: str, salt: bytes) -> bytes:
    """Derive a Fernet key from master password + salt."""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=480_000,
    )
    return base64.urlsafe_b64encode(kdf.derive(master_password.encode("utf-8")))


def _get_salt(config_path: Path) -> bytes:
    """Get or create salt file next to config."""
    salt_path = config_path.parent / (config_path.stem + ".salt")
    if salt_path.exists():
        return salt_path.read_bytes()
    salt = os.urandom(16)
    salt_path.write_bytes(salt)
    return salt


def is_encrypted(value: str) -> bool:
    """Check if a value is in ENC(...) format."""
    return isinstance(value, str) and value.startswith(_ENC_PREFIX) and value.endswith(_ENC_SUFFIX)


def encrypt_value(plaintext: str, master_password: str, salt: bytes) -> str:
    """Encrypt a plaintext value → ENC(base64-ciphertext)."""
    key = _derive_key(master_password, salt)
    f = Fernet(key)
    token = f.encrypt(plaintext.encode("utf-8"))
    return f"{_ENC_PREFIX}{token.decode('utf-8')}{_ENC_SUFFIX}"


def decrypt_value(enc_value: str, master_password: str, salt: bytes) -> str:
    """Decrypt ENC(base64-ciphertext) → plaintext. Raises InvalidToken on wrong password."""
    if not is_encrypted(enc_value):
        return enc_value
    token = enc_value[len(_ENC_PREFIX):-len(_ENC_SUFFIX)]
    key = _derive_key(master_password, salt)
    f = Fernet(key)
    return f.decrypt(token.encode("utf-8")).decode("utf-8")


def encrypt_passwords_in_yaml(raw_text: str, master_password: str, salt: bytes) -> str:
    """Find plaintext password values in YAML text and encrypt them.

    Matches lines like:  password: somevalue
    Skips lines that are already ENC(...) or environment variable references ($...).
    """
    import re
    lines = raw_text.split("\n")
    result = []
    for line in lines:
        m = re.match(r'^(\s*password:\s*)(.+)$', line)
        if m:
            prefix, value = m.group(1), m.group(2).strip()
            # Skip already encrypted, env vars, masked display, empty, or quoted empty
            if (is_encrypted(value) or value.startswith("$")
                    or value == "********" or not value or value in ('""', "''")):
                result.append(line)
            else:
                # Strip quotes if present
                clean = value.strip("'\"")
                enc = encrypt_value(clean, master_password, salt)
                result.append(f"{prefix}{enc}")
        else:
            result.append(line)
    return "\n".join(result)


def decrypt_passwords_in_config(data: dict, master_password: str, salt: bytes) -> dict:
    """Recursively decrypt ENC(...) values in a parsed config dict."""
    if isinstance(data, dict):
        return {k: decrypt_passwords_in_config(v, master_password, salt) for k, v in data.items()}
    if isinstance(data, list):
        return [decrypt_passwords_in_config(item, master_password, salt) for item in data]
    if isinstance(data, str) and is_encrypted(data):
        return decrypt_value(data, master_password, salt)
    return data
