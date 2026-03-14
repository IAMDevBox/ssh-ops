"""Tests for password encryption/decryption."""

import os
import tempfile
from pathlib import Path

import pytest

from ssh_ops.crypto import (
    _derive_key,
    _get_salt,
    decrypt_passwords_in_config,
    decrypt_value,
    encrypt_passwords_in_yaml,
    encrypt_value,
    is_encrypted,
)


@pytest.fixture
def salt():
    return os.urandom(16)


@pytest.fixture
def tmp_config(tmp_path):
    config = tmp_path / "test.yml"
    config.write_text("servers: []\n")
    return config


class TestIsEncrypted:
    def test_encrypted(self):
        assert is_encrypted("ENC(abc123)") is True

    def test_plain(self):
        assert is_encrypted("password123") is False

    def test_partial_prefix(self):
        assert is_encrypted("ENC(") is False

    def test_empty(self):
        assert is_encrypted("") is False

    def test_non_string(self):
        assert is_encrypted(123) is False


class TestEncryptDecrypt:
    def test_round_trip(self, salt):
        plaintext = "my-secret-password"
        encrypted = encrypt_value(plaintext, "master123", salt)
        assert is_encrypted(encrypted)
        assert encrypted != plaintext
        decrypted = decrypt_value(encrypted, "master123", salt)
        assert decrypted == plaintext

    def test_wrong_password_fails(self, salt):
        encrypted = encrypt_value("secret", "correct-password", salt)
        from cryptography.fernet import InvalidToken
        with pytest.raises(InvalidToken):
            decrypt_value(encrypted, "wrong-password", salt)

    def test_different_salts_different_ciphertext(self):
        salt1 = os.urandom(16)
        salt2 = os.urandom(16)
        enc1 = encrypt_value("same", "master", salt1)
        enc2 = encrypt_value("same", "master", salt2)
        assert enc1 != enc2

    def test_decrypt_plain_value_returns_unchanged(self, salt):
        assert decrypt_value("plain-text", "master", salt) == "plain-text"


class TestGetSalt:
    def test_creates_salt_file(self, tmp_config):
        salt = _get_salt(tmp_config)
        assert len(salt) == 16
        salt_path = tmp_config.parent / "test.salt"
        assert salt_path.exists()

    def test_reuses_existing_salt(self, tmp_config):
        salt1 = _get_salt(tmp_config)
        salt2 = _get_salt(tmp_config)
        assert salt1 == salt2


class TestEncryptPasswordsInYaml:
    def test_encrypts_plaintext_password(self, salt):
        yaml_text = "servers:\n  - host: example.com\n    password: secret123\n"
        result = encrypt_passwords_in_yaml(yaml_text, "master", salt)
        assert "secret123" not in result
        assert "ENC(" in result

    def test_skips_already_encrypted(self, salt):
        yaml_text = "servers:\n  - password: ENC(already-encrypted)\n"
        result = encrypt_passwords_in_yaml(yaml_text, "master", salt)
        assert result == yaml_text

    def test_skips_env_var(self, salt):
        yaml_text = "servers:\n  - password: $SSH_PASSWORD\n"
        result = encrypt_passwords_in_yaml(yaml_text, "master", salt)
        assert result == yaml_text

    def test_skips_masked(self, salt):
        yaml_text = "servers:\n  - password: ********\n"
        result = encrypt_passwords_in_yaml(yaml_text, "master", salt)
        assert result == yaml_text

    def test_multiple_passwords(self, salt):
        yaml_text = (
            "servers:\n"
            "  - host: a\n    password: pass1\n"
            "  - host: b\n    password: pass2\n"
            "  - host: c\n    password: $ENV_VAR\n"
        )
        result = encrypt_passwords_in_yaml(yaml_text, "master", salt)
        assert "pass1" not in result
        assert "pass2" not in result
        assert "$ENV_VAR" in result
        assert result.count("ENC(") == 2

    def test_preserves_indentation(self, salt):
        yaml_text = "    password: secret\n"
        result = encrypt_passwords_in_yaml(yaml_text, "master", salt)
        assert result.startswith("    password: ENC(")


class TestDecryptPasswordsInConfig:
    def test_decrypts_nested(self, salt):
        encrypted = encrypt_value("my-pass", "master", salt)
        config = {
            "servers": [
                {"host": "a", "password": encrypted},
                {"host": "b", "password": "$ENV"},
            ]
        }
        result = decrypt_passwords_in_config(config, "master", salt)
        assert result["servers"][0]["password"] == "my-pass"
        assert result["servers"][1]["password"] == "$ENV"

    def test_non_string_values_unchanged(self, salt):
        config = {"port": 22, "enabled": True, "items": [1, 2, 3]}
        result = decrypt_passwords_in_config(config, "master", salt)
        assert result == config
