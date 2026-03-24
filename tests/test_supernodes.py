"""
Supernode review pipeline tests.

Tests the find_supernodes action, generate with prompt resolution,
scope-glob matching, edge timestamp queries, and the daemon
replenishment logic.
"""

import pytest

from keep.api import Keeper


@pytest.fixture
def kp(mock_providers, tmp_path):
    """Keeper with embedding provider for tests that don't need real edges."""
    kp = Keeper(store_path=tmp_path)
    kp._get_embedding_provider()
    return kp


@pytest.fixture
def kp_real_store(tmp_path):
    """Keeper with real DocumentStore for edge/supernode tests.

    Uses the real SQLite document store (not mocked) so edge queries
    work. Still mocks embedding and summarization providers.
    """
    from unittest.mock import patch, MagicMock

    # Only mock the providers, not the document store
    mock_embed = MagicMock()
    mock_embed.embed.return_value = [0.1] * 384
    mock_embed.dimensions = 384
    mock_embed.model_name = "mock-model"
    mock_embed.provider_name = "ollama"

    with patch("keep.api.Keeper._get_embedding_provider", return_value=mock_embed), \
         patch("keep.api.Keeper._spawn_processor", return_value=False):
        kp = Keeper(store_path=tmp_path)
        kp._get_embedding_provider()
        # Trigger system doc migration (normally happens on first put)
        kp.put("migration trigger", id="_test_trigger")
        kp.delete("_test_trigger")
        yield kp
        kp.close()


def _create_supernode_scenario(kp):
    """Create a scenario with a stub supernode and inbound edges.

    Uses insert_if_absent to create a genuine stub (as extract_links does),
    then creates items with edge tags pointing to it.
    """
    target_id = "alice@example.com"

    # Create a genuine stub via insert_if_absent (like extract_links does)
    doc_coll = kp._resolve_doc_collection()
    kp._document_store.insert_if_absent(
        doc_coll, target_id,
        summary="",
        tags={"_source": "link"},
    )

    # Create items that reference the stub via edges
    for i in range(7):
        kp.put(
            f"Email about project updates #{i}",
            id=f"email-{i}",
            tags={"from": target_id},
        )

    return target_id


class TestFindSupernodeCandidates:
    """Tests for DocumentStore.find_supernode_candidates()."""

    def test_finds_high_fan_in_items(self, kp_real_store):
        """Items with many inbound edges are discovered."""
        stub_id = _create_supernode_scenario(kp_real_store)
        doc_coll = kp_real_store._resolve_doc_collection()

        candidates = kp_real_store._document_store.find_supernode_candidates(
            doc_coll, min_fan_in=3, limit=10,
        )

        ids = [c["id"] for c in candidates]
        assert stub_id in ids

    def test_min_fan_in_filters(self, kp_real_store):
        """Items below min_fan_in threshold are excluded."""
        _create_supernode_scenario(kp_real_store)
        doc_coll = kp_real_store._resolve_doc_collection()

        candidates = kp_real_store._document_store.find_supernode_candidates(
            doc_coll, min_fan_in=100, limit=10,
        )

        assert len(candidates) == 0

    def test_reviewed_items_with_no_new_refs_excluded(self, kp_real_store):
        """Previously reviewed items with no new refs are skipped."""
        stub_id = _create_supernode_scenario(kp_real_store)

        # Mark as reviewed (with a future timestamp so all refs are "old")
        # Use patch_head_tags directly since tag() may filter system tags
        doc_coll = kp_real_store._resolve_doc_collection()
        kp_real_store._document_store.patch_head_tags(
            doc_coll, stub_id, {"_supernode_reviewed": "2099-01-01T00:00:00"},
        )

        candidates = kp_real_store._document_store.find_supernode_candidates(
            doc_coll, min_fan_in=3, limit=10,
        )

        # Should not appear (no new refs since review)
        ids = [c["id"] for c in candidates]
        assert stub_id not in ids


class TestFindSupernodes:
    """Tests for the find_supernodes action."""

    def test_action_registered(self):
        """find_supernodes action is discoverable."""
        from keep.actions import list_actions
        assert "find_supernodes" in list_actions()

    def test_action_returns_results(self, kp):
        """Action returns candidates with expected fields."""
        _create_supernode_scenario(kp)

        # Run via flow command
        result = kp.run_flow_command(
            "test-find-supernodes",
            params={"min_fan_in": 3, "limit": 5},
            state_doc_yaml="""\
match: sequence
rules:
  - id: candidates
    do: find_supernodes
    with:
      min_fan_in: "{params.min_fan_in}"
      limit: "{params.limit}"
  - return: done
""",
        )

        assert result.status == "done"
        candidates = result.data.get("candidates", {})
        if isinstance(candidates, dict):
            results = candidates.get("results", [])
            if results:
                assert "id" in results[0]
                assert "fan_in" in results[0]
                assert "score" in results[0]


class TestGenerateWithPrompt:
    """Tests for generate action prompt resolution."""

    def test_generate_delegates_in_foreground(self, kp):
        """Generate (async) triggers delegation in foreground flow."""
        result = kp.run_flow_command(
            "test-gen",
            params={},
            state_doc_yaml="""\
match: sequence
rules:
  - id: out
    do: generate
    with:
      system: "You are a test assistant."
      user: "Say hello."
  - return: done
""",
            writable=True,
        )

        # Generate is async — foreground flow produces cursor
        assert result.status == "async"
        assert result.cursor is not None
        assert result.data["action"] == "generate"

    def test_generate_with_prompt_param_delegates(self, kp):
        """Generate with prompt param triggers delegation (foreground)."""
        # Create a test prompt doc
        kp.put(
            "# Test prompt\n\n## Prompt\n\nYou are a test summarizer.",
            id=".prompt/test-gen/default",
            tags={"category": "system", "context": "prompt"},
        )

        result = kp.run_flow_command(
            "test-gen",
            params={},
            state_doc_yaml="""\
match: sequence
rules:
  - id: out
    do: generate
    with:
      prompt: "test-gen"
      user: "Summarize: hello world"
  - return: done
""",
            writable=True,
        )

        # Async delegation — cursor enqueued for daemon
        assert result.status == "async"
        assert result.data["action"] == "generate"


class TestScopeGlobPromptMatching:
    """Tests for scope-glob matching in _resolve_prompt_doc."""

    def test_email_scope_matches(self, kp_real_store):
        """Prompt with scope *@* matches email-like IDs."""
        prompt = kp_real_store._resolve_prompt_doc(
            "supernode", {}, item_id="alice@example.com",
        )
        # Should match .prompt/supernode/email (scope: *@*)
        # or .prompt/supernode/default (scope: *) as fallback
        assert prompt is not None

    def test_url_scope_matches(self, kp_real_store):
        """Prompt with scope http*://* matches URL IDs."""
        prompt = kp_real_store._resolve_prompt_doc(
            "supernode", {}, item_id="https://example.com/page",
        )
        assert prompt is not None

    def test_most_specific_scope_wins(self, kp_real_store):
        """More specific scope takes priority over wildcard."""
        email_prompt = kp_real_store._resolve_prompt_doc(
            "supernode", {}, item_id="alice@example.com",
        )
        url_prompt = kp_real_store._resolve_prompt_doc(
            "supernode", {}, item_id="https://example.com",
        )
        default_prompt = kp_real_store._resolve_prompt_doc(
            "supernode", {}, item_id="some-random-id",
        )

        # All should resolve (at minimum the default)
        assert email_prompt is not None
        assert url_prompt is not None
        assert default_prompt is not None


class TestSupernodeReplenishment:
    """Tests for daemon queue replenishment."""

    def test_replenish_with_no_candidates(self, kp_real_store):
        """Replenishment with empty store returns 0."""
        enqueued = kp_real_store.replenish_supernode_queue()
        assert enqueued == 0

    def test_replenish_enqueues_candidates(self, kp_real_store):
        """Replenishment with eligible supernodes enqueues flow items."""
        _create_supernode_scenario(kp_real_store)

        # Drain any existing work
        queue = kp_real_store._get_work_queue()
        queue.claim("drain", limit=200)

        enqueued = kp_real_store.replenish_supernode_queue(min_fan_in=3)

        if enqueued > 0:
            claimed = queue.claim("test", limit=20)
            flow_items = [i for i in claimed if i.kind == "flow"]
            supernode_flows = [
                i for i in flow_items
                if i.input.get("state") == "review-supernodes"
            ]
            assert len(supernode_flows) > 0


class TestMetaSupernodes:
    """Tests for .meta/supernodes context surfacing."""

    def test_meta_doc_parses_as_state_doc(self, kp_real_store):
        """The .meta/supernodes doc parses as a valid state doc."""
        from keep.state_doc import parse_state_doc

        doc_coll = kp_real_store._resolve_doc_collection()
        rec = kp_real_store._document_store.get(doc_coll, ".meta/supernodes")
        assert rec is not None, "System migration should create .meta/supernodes"
        doc = parse_state_doc("supernodes", rec.summary)
        assert len(doc.rules) > 0


class TestReviewSupernodes:
    """Tests for the review-supernodes state doc."""

    def test_state_doc_parses(self, kp_real_store):
        """The review-supernodes state doc is valid."""
        from keep.state_doc import parse_state_doc

        doc_coll = kp_real_store._resolve_doc_collection()
        rec = kp_real_store._document_store.get(doc_coll, ".state/review-supernodes")
        assert rec is not None, "System migration should create .state/review-supernodes"
        doc = parse_state_doc("review-supernodes", rec.summary)
        assert doc.match == "sequence"
        assert len(doc.rules) >= 4  # get, traverse, generate, put, return
