"""Tests for thin CLI pending command lifecycle behavior."""

from unittest.mock import patch

from keep import thin_cli
from keep.const import DAEMON_PORT_FILE, DAEMON_TOKEN_FILE


def test_pending_stop_cleans_stale_discovery_files_without_pid(tmp_path, capsys):
    store = tmp_path / "store"
    store.mkdir()
    (store / DAEMON_PORT_FILE).write_text("5337")
    (store / DAEMON_TOKEN_FILE).write_text("token")

    with patch("keep.daemon_client.resolve_store_path", return_value=store):
        thin_cli.pending(stop=True)

    captured = capsys.readouterr()
    assert "No daemon running." in captured.out
    assert not (store / DAEMON_PORT_FILE).exists()
    assert not (store / DAEMON_TOKEN_FILE).exists()
