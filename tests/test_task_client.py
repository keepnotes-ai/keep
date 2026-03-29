"""Tests for keep.task_client — HTTP client for the Task API."""

import json
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
import httpx

from keep.task_client import TaskClient, TaskClientError


class FakeResponse:
    """Minimal httpx.Response stand-in."""

    def __init__(self, status_code=200, json_data=None, text="", headers=None):
        self.status_code = status_code
        self._json = json_data or {}
        self.text = text
        self.headers = headers or {}

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"{self.status_code}",
                request=httpx.Request("GET", "http://test"),
                response=self,
            )


@pytest.fixture
def mock_client():
    """TaskClient with a mocked httpx.Client."""
    with patch("keep.task_client.httpx.Client") as MockClient:
        client_instance = MagicMock()
        MockClient.return_value = client_instance
        tc = TaskClient("https://api.example.com", "test-key", project="myproject")
        yield tc, client_instance


class TestHTTPSEnforcement:
    """Tests for HTTPS enforcement."""
    def test_allows_https(self):
        with patch("keep.task_client.httpx.Client"):
            tc = TaskClient("https://api.example.com", "key")
            assert tc._api_url == "https://api.example.com"

    def test_allows_localhost(self):
        with patch("keep.task_client.httpx.Client"):
            tc = TaskClient("http://localhost:8000", "key")
            assert tc._api_url == "http://localhost:8000"

    def test_allows_127_0_0_1(self):
        with patch("keep.task_client.httpx.Client"):
            tc = TaskClient("http://127.0.0.1:8000", "key")
            assert tc._api_url == "http://127.0.0.1:8000"

    def test_rejects_plain_http(self):
        with pytest.raises(ValueError, match="must use HTTPS"):
            TaskClient("http://api.example.com", "key")


class TestDiscoverProcessors:
    """Tests for processor discovery."""
    def test_returns_task_types(self, mock_client):
        tc, http = mock_client
        http.get.return_value = FakeResponse(json_data={
            "processors": [
                {"task_type": "summarize", "description": "Summarize text"},
                {"task_type": "ocr", "description": "OCR images"},
            ]
        })

        result = tc.discover_processors()

        assert result == ["summarize", "ocr"]
        http.get.assert_called_once_with("/v1/processors")

    def test_returns_empty_on_error(self, mock_client):
        tc, http = mock_client
        http.get.side_effect = httpx.ConnectError("down")

        result = tc.discover_processors()

        assert result == []


class TestSubmit:
    """Tests for task submission."""
    def test_submit_returns_task_id(self, mock_client):
        tc, http = mock_client
        http.post.return_value = FakeResponse(
            status_code=202,
            json_data={"task_id": "task-123", "status": "queued"},
        )

        task_id = tc.submit("summarize", "some content")

        assert task_id == "task-123"
        call_args = http.post.call_args
        assert call_args[0][0] == "/v1/tasks"
        payload = call_args[1]["json"]
        assert payload["task_type"] == "summarize"
        assert payload["content"] == "some content"

    def test_submit_with_metadata(self, mock_client):
        tc, http = mock_client
        http.post.return_value = FakeResponse(
            status_code=202,
            json_data={"task_id": "task-456"},
        )

        tc.submit("ocr", "image data", metadata={"context": "receipt"})

        payload = http.post.call_args[1]["json"]
        assert payload["metadata"] == {"context": "receipt"}

    def test_submit_with_action_contract(self, mock_client):
        tc, http = mock_client
        http.post.return_value = FakeResponse(
            status_code=202,
            json_data={"task_id": "task-action"},
        )

        tc.submit(
            "summarize",
            "some content",
            metadata={"context": "receipt"},
            action_name="summarize",
            action_params={"item_id": "doc1", "context": "receipt"},
        )

        payload = http.post.call_args[1]["json"]
        assert payload["action"] == {
            "name": "summarize",
            "params": {"item_id": "doc1", "context": "receipt"},
        }

    def test_submit_retries_on_5xx(self, mock_client):
        tc, http = mock_client

        resp_500 = FakeResponse(status_code=500, text="Internal Server Error")
        resp_ok = FakeResponse(status_code=202, json_data={"task_id": "task-789"})
        http.post.side_effect = [resp_500, resp_ok]

        with patch("keep.task_client.time.sleep"):
            task_id = tc.submit("summarize", "content")

        assert task_id == "task-789"
        assert http.post.call_count == 2

    def test_submit_retries_on_429(self, mock_client):
        tc, http = mock_client

        resp_429 = FakeResponse(
            status_code=429, text="Too Many Requests",
            headers={"Retry-After": "1"},
        )
        # 429 doesn't raise_for_status in our flow — it's checked before
        # Override raise_for_status to not raise for 429
        resp_429.raise_for_status = lambda: None
        resp_ok = FakeResponse(status_code=202, json_data={"task_id": "task-abc"})
        http.post.side_effect = [resp_429, resp_ok]

        with patch("keep.task_client.time.sleep"):
            task_id = tc.submit("summarize", "content")

        assert task_id == "task-abc"

    def test_submit_caps_retry_after_at_60s(self, mock_client):
        tc, http = mock_client

        resp_429 = FakeResponse(
            status_code=429, text="Too Many Requests",
            headers={"Retry-After": "3600"},
        )
        resp_429.raise_for_status = lambda: None
        resp_ok = FakeResponse(status_code=202, json_data={"task_id": "task-cap"})
        http.post.side_effect = [resp_429, resp_ok]

        with patch("keep.task_client.time.sleep") as mock_sleep:
            task_id = tc.submit("summarize", "content")

        assert task_id == "task-cap"
        # Should have slept 60s, not 3600s
        mock_sleep.assert_called_once_with(60.0)

    def test_submit_raises_on_4xx(self, mock_client):
        tc, http = mock_client
        http.post.return_value = FakeResponse(
            status_code=400, text="Bad Request"
        )

        with pytest.raises(TaskClientError, match="rejected"):
            tc.submit("summarize", "content")

    def test_submit_raises_after_max_retries(self, mock_client):
        tc, http = mock_client
        http.post.side_effect = httpx.ConnectError("connection refused")

        with patch("keep.task_client.time.sleep"):
            with pytest.raises(TaskClientError, match="failed after"):
                tc.submit("summarize", "content")

        assert http.post.call_count == 3  # MAX_RETRIES


class TestPoll:
    """Tests for task polling."""
    def test_poll_completed(self, mock_client):
        tc, http = mock_client
        http.get.return_value = FakeResponse(json_data={
            "task_id": "task-123",
            "status": "completed",
            "task_type": "summarize",
            "result": {"summary": "A brief summary"},
        })

        result = tc.poll("task-123")

        assert result["status"] == "completed"
        assert result["result"]["summary"] == "A brief summary"
        assert result["output"] is None

    def test_poll_completed_with_action_output(self, mock_client):
        tc, http = mock_client
        http.get.return_value = FakeResponse(json_data={
            "task_id": "task-123",
            "status": "completed",
            "task_type": "summarize",
            "output": {
                "summary": "A brief summary",
                "mutations": [
                    {"op": "set_summary", "target": "doc1", "summary": "A brief summary"},
                ],
            },
        })

        result = tc.poll("task-123")

        assert result["status"] == "completed"
        assert result["output"]["summary"] == "A brief summary"
        assert result["result"] is None

    def test_poll_processing(self, mock_client):
        tc, http = mock_client
        http.get.return_value = FakeResponse(json_data={
            "task_id": "task-123",
            "status": "processing",
        })

        result = tc.poll("task-123")

        assert result["status"] == "processing"
        assert result["result"] is None

    def test_poll_failed(self, mock_client):
        tc, http = mock_client
        http.get.return_value = FakeResponse(json_data={
            "task_id": "task-123",
            "status": "failed",
            "error": "Model crashed",
        })

        result = tc.poll("task-123")

        assert result["status"] == "failed"
        assert result["error"] == "Model crashed"

    def test_poll_404_returns_not_found(self, mock_client):
        tc, http = mock_client
        http.get.return_value = FakeResponse(status_code=404)

        result = tc.poll("nonexistent")

        assert result["status"] == "not_found"

    def test_poll_raises_on_server_error(self, mock_client):
        tc, http = mock_client
        http.get.return_value = FakeResponse(status_code=500)

        with pytest.raises(TaskClientError, match="Poll failed"):
            tc.poll("task-123")


class TestAcknowledge:
    """Tests for task acknowledgment."""
    def test_acknowledge_success(self, mock_client):
        tc, http = mock_client
        http.delete.return_value = FakeResponse(
            json_data={"deleted": True, "task_id": "task-123"}
        )

        tc.acknowledge("task-123")

        http.delete.assert_called_once_with("/v1/tasks/task-123")

    def test_acknowledge_404_is_fine(self, mock_client):
        tc, http = mock_client
        http.delete.return_value = FakeResponse(status_code=404)

        # Should not raise
        tc.acknowledge("already-gone")

    def test_acknowledge_connection_error_is_non_critical(self, mock_client):
        tc, http = mock_client
        http.delete.side_effect = httpx.ConnectError("down")

        # Should not raise
        tc.acknowledge("task-123")


class TestAvailable:
    """Tests for processor availability check."""
    def test_available_when_processors_exist(self, mock_client):
        tc, http = mock_client
        http.get.return_value = FakeResponse(json_data={
            "processors": [{"task_type": "summarize", "description": "..."}]
        })

        assert tc.available is True

    def test_not_available_when_no_processors(self, mock_client):
        tc, http = mock_client
        http.get.return_value = FakeResponse(json_data={"processors": []})

        assert tc.available is False

    def test_not_available_on_error(self, mock_client):
        tc, http = mock_client
        http.get.side_effect = httpx.ConnectError("down")

        assert tc.available is False

    def test_caches_result(self, mock_client):
        tc, http = mock_client
        http.get.return_value = FakeResponse(json_data={
            "processors": [{"task_type": "summarize", "description": "..."}]
        })

        _ = tc.available
        _ = tc.available  # Second call should use cache

        assert http.get.call_count == 1
