"""Shared test fixtures — isolate tests from the real global.yml."""

from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _isolate_global_config():
    """Prevent tests from loading the real global.yml.

    The production global.yml enables auto_backup, server_warn_patterns, etc.
    Loading it during tests causes mock sessions to hang in _exhaust_generator
    because MagicMock.__next__ never raises StopIteration.
    """
    with patch("ssh_ops.config._load_global_config", return_value={}):
        yield
