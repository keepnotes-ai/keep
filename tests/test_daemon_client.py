"""Tests for daemon client discovery and auto-start logic."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from keep.const import DAEMON_PORT_FILE, DAEMON_TOKEN_FILE


class TestGetPortNoFileStranding:
    """get_port must not delete discovery files of a still-running daemon."""

    def test_unhealthy_daemon_files_not_deleted(self, tmp_path):
        """When health check fails, discovery files are preserved for recovery.

        Previously, get_port() deleted .daemon.port and .daemon.token
        immediately on health-check failure, then spawned a replacement.
        If the original daemon was alive but briefly unhealthy and the
        replacement exited on .processor.lock, both files were gone and
        no daemon was reachable.
        """
        store = tmp_path / "store"
        store.mkdir()
        port_file = store / DAEMON_PORT_FILE
        token_file = store / DAEMON_TOKEN_FILE
        port_file.write_text("9999")
        token_file.write_text("tok-abc")

        health_calls = []

        def mock_health(port):
            health_calls.append(port)
            # First call: unhealthy.  Second call: recovered.
            return len(health_calls) > 1

        with (
            patch("keep._daemon_client.resolve_store_path", return_value=store),
            patch("keep._daemon_client.check_health", side_effect=mock_health),
            patch("keep._daemon_client.start_daemon"),
            patch("keep._daemon_client._load_token"),
        ):
            from keep._daemon_client import get_port
            port = get_port(str(store))

        assert port == 9999
        # Files must still exist — not deleted
        assert port_file.exists()
        assert token_file.exists()

    def test_dead_daemon_replacement_writes_new_files(self, tmp_path):
        """When old daemon is truly dead, replacement writes new discovery files."""
        store = tmp_path / "store"
        store.mkdir()
        port_file = store / DAEMON_PORT_FILE
        token_file = store / DAEMON_TOKEN_FILE
        # Stale files from dead daemon
        port_file.write_text("8888")
        token_file.write_text("old-token")

        health_calls = []

        def mock_health(port):
            health_calls.append(port)
            # Old port 8888 always unhealthy; new port 7777 healthy
            return port == 7777

        def mock_start(store_path):
            # Simulate replacement daemon writing new files
            port_file.write_text("7777")
            token_file.write_text("new-token")

        with (
            patch("keep._daemon_client.resolve_store_path", return_value=store),
            patch("keep._daemon_client.check_health", side_effect=mock_health),
            patch("keep._daemon_client.start_daemon", side_effect=mock_start),
            patch("keep._daemon_client._load_token"),
        ):
            from keep._daemon_client import get_port
            port = get_port(str(store))

        assert port == 7777

    def test_no_existing_daemon_starts_fresh(self, tmp_path):
        """With no discovery files, get_port spawns a daemon and polls."""
        store = tmp_path / "store"
        store.mkdir()
        port_file = store / DAEMON_PORT_FILE

        def mock_start(store_path):
            port_file.write_text("5555")

        def mock_health(port):
            return port == 5555

        with (
            patch("keep._daemon_client.resolve_store_path", return_value=store),
            patch("keep._daemon_client.check_health", side_effect=mock_health),
            patch("keep._daemon_client.start_daemon", side_effect=mock_start),
            patch("keep._daemon_client._load_token"),
        ):
            from keep._daemon_client import get_port
            port = get_port(str(store))

        assert port == 5555


class TestLoadTokenCacheScoping:
    """Token cache must be scoped to the resolved store path."""

    def test_load_token_switches_stores_without_force(self, tmp_path):
        store_a = tmp_path / "store-a"
        store_b = tmp_path / "store-b"
        store_a.mkdir()
        store_b.mkdir()
        (store_a / DAEMON_TOKEN_FILE).write_text("token-a")
        (store_b / DAEMON_TOKEN_FILE).write_text("token-b")

        from keep import _daemon_client as client

        client._auth_token = ""
        client._auth_token_store = ""
        try:
            token_a = client._load_token(str(store_a))
            token_b = client._load_token(str(store_b))

            assert token_a == "token-a"
            assert token_b == "token-b"
            assert client._auth_token_store == str(store_b.resolve())
        finally:
            client._auth_token = ""
            client._auth_token_store = ""
