"""Remote flow transport delegation tests."""

from unittest.mock import MagicMock, patch

import pytest

from keep.config import StoreConfig
from keep.remote import RemoteKeeper


class FakeResponse:
    """Minimal httpx.Response stand-in."""

    def __init__(self, status_code=200, json_data=None):
        self.status_code = status_code
        self._json = {} if json_data is None else json_data

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


@pytest.fixture
def mock_remote_client(tmp_path):
    with patch("keep.remote.httpx.Client") as mock_cls:
        client = MagicMock()
        mock_cls.return_value = client
        rk = RemoteKeeper(
            "https://api.example.com",
            "test-key",
            StoreConfig(path=tmp_path),
        )
        try:
            yield rk, client
        finally:
            rk.close()


def test_remote_continue_flow_delegates_to_http(mock_remote_client):
    rk, client = mock_remote_client
    payload = {"request_id": "req-1", "goal": "query", "work_results": []}
    client.post.return_value = FakeResponse(
        json_data={"cursor": "c_123", "status": "done"},
    )

    out = rk.continue_flow(payload)

    assert out["cursor"] == "c_123"
    client.post.assert_called_once_with("/v1/continue", json=payload)


def test_remote_continue_run_work_delegates_to_http(mock_remote_client):
    rk, client = mock_remote_client
    client.post.return_value = FakeResponse(
        json_data={"work_id": "w_1", "status": "ok", "outputs": {"summary": "x"}},
    )

    out = rk.continue_run_work("c_1", "w_1")

    assert out["work_id"] == "w_1"
    client.post.assert_called_once_with(
        "/v1/continue/work",
        json={"cursor": "c_1", "work_id": "w_1"},
    )


def test_remote_continue_flow_rejects_non_object(mock_remote_client):
    rk, _ = mock_remote_client
    with pytest.raises(ValueError, match="JSON object"):
        rk.continue_flow("bad-input")  # type: ignore[arg-type]


def test_remote_continue_work_validates_ids(mock_remote_client):
    rk, _ = mock_remote_client
    with pytest.raises(ValueError, match="required"):
        rk.continue_run_work("", "w_1")
    with pytest.raises(ValueError, match="required"):
        rk.continue_run_work("c_1", "")


def test_remote_continue_flow_requires_object_response(mock_remote_client):
    rk, client = mock_remote_client
    client.post.return_value = FakeResponse(json_data=[{"cursor": "c_123"}])

    with pytest.raises(ValueError, match="must be a JSON object"):
        rk.continue_flow({"request_id": "req-1"})
