"""Shared contract tests for flow-host implementations.

These tests exercise the stable wrapper-level contract against:
- local in-process Keeper
- a thin raw HTTP /v1/flow host over the local daemon
- RemoteKeeper over that same daemon
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from keep.api import Keeper
from keep.config import StoreConfig
from keep.daemon_server import DaemonServer
from keep.flow_client import (
    delete_item,
    find_items,
    get_item,
    get_now_item,
    put_item,
    set_now_item,
    tag_item,
)
from keep.protocol import FlowHostProtocol
from keep.remote import RemoteKeeper
from keep.state_doc_runtime import FlowResult


class HttpFlowHost:
    """Minimal raw HTTP flow host for contract testing."""

    def __init__(self, base_url: str, auth_token: str):
        self._client = httpx.Client(
            base_url=base_url,
            headers={"Authorization": f"Bearer {auth_token}"},
            timeout=5,
        )

    def run_flow(
        self,
        state: str,
        *,
        params: dict[str, Any] | None = None,
        budget: int | None = None,
        cursor_token: str | None = None,
        state_doc_yaml: str | None = None,
        writable: bool = True,
    ) -> FlowResult:
        resp = self._client.post("/v1/flow", json={
            "state": state,
            "params": params,
            "budget": budget,
            "cursor_token": cursor_token,
            "state_doc_yaml": state_doc_yaml,
            "writable": writable,
        })
        resp.raise_for_status()
        data = resp.json()
        return FlowResult(
            status=data.get("status", "error"),
            bindings=data.get("bindings", {}),
            data=data.get("data"),
            ticks=data.get("ticks", 0),
            history=data.get("history", []),
            cursor=data.get("cursor"),
        )

    def close(self) -> None:
        self._client.close()


@pytest.fixture
def daemon_runtime(mock_providers, tmp_path: Path) -> Iterator[tuple[DaemonServer, Keeper, int]]:
    keeper = Keeper(store_path=tmp_path)
    server = DaemonServer(keeper, port=0)
    port = server.start()
    try:
        yield server, keeper, port
    finally:
        server.stop()
        keeper.close()


@pytest.fixture(params=["local", "daemon-http", "remote"])
def flow_host(
    request: pytest.FixtureRequest,
    mock_providers,
    tmp_path: Path,
    daemon_runtime: tuple[DaemonServer, Keeper, int],
) -> Iterator[FlowHostProtocol]:
    mode = request.param
    if mode == "local":
        keeper = Keeper(store_path=tmp_path / "local-store")
        try:
            yield keeper
        finally:
            keeper.close()
        return

    server, daemon_keeper, port = daemon_runtime
    if mode == "daemon-http":
        host = HttpFlowHost(f"http://127.0.0.1:{port}", server.auth_token)
        try:
            yield host
        finally:
            host.close()
        return

    if mode == "remote":
        client = RemoteKeeper(
            api_url=f"http://127.0.0.1:{port}",
            api_key=server.auth_token,
            config=StoreConfig(path=daemon_keeper.config.path),
        )
        try:
            yield client
        finally:
            client.close()
        return

    raise AssertionError(f"unknown flow host mode: {mode}")


def test_flow_host_contract_core_memory_ops(flow_host: FlowHostProtocol):
    item = put_item(
        flow_host,
        "Contract body text",
        id="contract-1",
        tags={"topic": "contract"},
    )
    assert item.id == "contract-1"

    fetched = get_item(flow_host, "contract-1")
    assert fetched is not None
    assert fetched.id == "contract-1"

    results = find_items(flow_host, query="Contract body", limit=5)
    assert any(result.id == "contract-1" for result in results)

    tagged = tag_item(flow_host, "contract-1", {"status": "open"})
    assert tagged is not None
    assert tagged.tags.get("status") == "open"

    listed = find_items(flow_host, tags={"status": "open"}, limit=10)
    assert any(result.id == "contract-1" for result in listed)

    assert delete_item(flow_host, "contract-1") is True
    assert get_item(flow_host, "contract-1") is None


def test_flow_host_contract_now_semantics(flow_host: FlowHostProtocol):
    now_item = get_now_item(flow_host)
    assert now_item.id == "now"

    updated = set_now_item(
        flow_host,
        "Contract now content",
        tags={"topic": "contract"},
    )
    assert updated.id == "now"

    fetched = get_now_item(flow_host)
    assert fetched.id == "now"
    assert "Contract now content" in fetched.summary or fetched.tags.get("topic") == "contract"


def test_flow_host_contract_named_writable_flows(flow_host: FlowHostProtocol):
    result = flow_host.run_flow("put", params={"content": "via flow", "id": "flow-contract"})
    assert result.status == "done"
    assert get_item(flow_host, "flow-contract") is not None

    result = flow_host.run_flow("tag", params={"id": "flow-contract", "tags": {"status": "queued"}})
    assert result.status == "done"
    tagged = get_item(flow_host, "flow-contract")
    assert tagged is not None
    assert tagged.tags.get("status") == "queued"

    result = flow_host.run_flow("delete", params={"id": "flow-contract"})
    assert result.status == "done"
    assert get_item(flow_host, "flow-contract") is None
