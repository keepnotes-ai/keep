"""Test _EnvActionContext method coverage for write-path actions.

Regression tests for missing methods on _EnvActionContext that exist
on _KeeperActionContext but were never wired through.
"""

from types import SimpleNamespace
from keep.state_doc_runtime import _EnvActionContext
from keep.actions.resolve_duplicates import ResolveDuplicates


def _make_doc(id, content_hash, content_hash_full="", tags=None):
    return SimpleNamespace(
        id=id,
        content_hash=content_hash,
        content_hash_full=content_hash_full,
        tags=tags or {},
    )


class FakeDocumentStore:
    def __init__(self, records):
        self._records = records

    def find_by_content_hash(self, collection, content_hash, *,
                             content_hash_full="", exclude_id="", limit=1):
        matches = [
            r for r in self._records
            if r.content_hash == content_hash and r.id != exclude_id
        ]
        if content_hash_full:
            matches = [
                r for r in matches
                if not r.content_hash_full or r.content_hash_full == content_hash_full
            ]
        return matches[:limit] if limit > 1 else (matches[0] if matches else None)


class FakeEnv:
    def __init__(self, docs=None):
        docs = docs or []
        self._docs = {d.id: d for d in docs}
        self._document_store = FakeDocumentStore(docs)
        self._collection = "default"
        self._media_provider = None

    def get_document(self, id):
        return self._docs.get(id)

    def get_document_store(self):
        return self._document_store

    def get_collection(self):
        return self._collection

    def get_default_media_provider(self):
        return self._media_provider


# --- resolve_duplicates through _EnvActionContext ---

def test_find_by_content_hash_exposed():
    """_EnvActionContext exposes find_by_content_hash for resolve_duplicates."""
    doc_a = _make_doc("a", "abc123", "full_abc")
    doc_b = _make_doc("b", "abc123", "full_abc")
    env = FakeEnv([doc_a, doc_b])
    ctx = _EnvActionContext(env, writable=True, item_id="a")

    action = ResolveDuplicates()
    result = action.run({"item_id": "a"}, ctx)

    assert result["duplicates"] == ["b"]
    assert len(result["mutations"]) == 1
    assert result["mutations"][0]["tags"]["duplicates"] == "b"


def test_find_by_content_hash_no_matches():
    """No duplicates returns empty list, no mutations."""
    doc_a = _make_doc("a", "abc123")
    env = FakeEnv([doc_a])
    ctx = _EnvActionContext(env, writable=True, item_id="a")

    action = ResolveDuplicates()
    result = action.run({"item_id": "a"}, ctx)

    assert result["duplicates"] == []
    assert "mutations" not in result


# --- resolve_provider("media") through _EnvActionContext ---

def test_resolve_provider_media():
    """resolve_provider('media') delegates to env.get_default_media_provider."""
    sentinel = object()
    env = FakeEnv()
    env._media_provider = sentinel
    ctx = _EnvActionContext(env, writable=True)

    result = ctx.resolve_provider("media")
    assert result is sentinel


def test_resolve_provider_media_none():
    """resolve_provider('media') returns None when no provider configured."""
    env = FakeEnv()
    ctx = _EnvActionContext(env, writable=True)

    result = ctx.resolve_provider("media")
    assert result is None
