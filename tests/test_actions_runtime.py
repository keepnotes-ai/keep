from keep.api import Keeper
from keep.api import _text_content_id
from keep.providers.base import Document


def _continue_with_single_step(request_id: str, step: dict, *, params: dict | None = None) -> dict:
    return {
        "request_id": request_id,
        "goal": "actions.test",
        "params": dict(params or {}),
        "steps": [step],
        "work_results": [],
    }


def test_action_summarize_applies_output_mutations(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "state actions summarize " * 60
        kp.put(content=content, id="note:action:summarize", summary="placeholder")

        first = kp.continue_flow(
            _continue_with_single_step(
                "act-summarize-1",
                {
                    "kind": "summarize",
                    "runner": {"type": "action.summarize"},
                    "input_mode": "note_content",
                    "output_contract": {"must_return": ["summary"]},
                },
                params={"id": "note:action:summarize", "content": content},
            )
        )
        assert first["status"] == "waiting_work"
        work_id = first["work"][0]["work_id"]

        work_result = kp.continue_run_work(first["cursor"], work_id)
        assert "mutations" in work_result["outputs"]
        second = kp.continue_flow(
            {
                "request_id": "act-summarize-2",
                "cursor": first["cursor"],
                "work_results": [work_result],
            }
        )
        assert second["status"] == "done"

        item = kp.get("note:action:summarize")
        assert item is not None
        assert item.summary == content[:200]
    finally:
        kp.close()


def test_action_put_applies_output_mutations_without_apply_block(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "hello from action put"
        expected_id = _text_content_id(content)
        first = kp.continue_flow(
            _continue_with_single_step(
                "act-put-1",
                {
                    "kind": "put",
                    "runner": {
                        "type": "action.put",
                        "params": {
                            "content": content,
                            "tags": {"topic": "actions"},
                            "queue_background_tasks": False,
                        },
                    },
                    "input_mode": "none",
                    "output_contract": {"must_return": ["id"]},
                },
            )
        )
        assert first["status"] == "waiting_work"
        work_id = first["work"][0]["work_id"]
        work_result = kp.continue_run_work(first["cursor"], work_id)
        assert work_result["outputs"]["id"] == expected_id

        second = kp.continue_flow(
            {
                "request_id": "act-put-2",
                "cursor": first["cursor"],
                "work_results": [work_result],
            }
        )
        assert second["status"] == "done"
        created = kp.get(expected_id)
        assert created is not None
        assert created.tags.get("topic") == "actions"
    finally:
        kp.close()


def test_action_find_supports_created_order(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="one", id="note:created:1", tags={"topic": "ordered"}, summary="one")
        kp.put(content="two", id="note:created:2", tags={"topic": "ordered"}, summary="two")
        first = kp.continue_flow(
            _continue_with_single_step(
                "act-find-1",
                {
                    "kind": "find",
                    "runner": {
                        "type": "action.find",
                        "params": {
                            "tags": {"topic": "ordered"},
                            "order_by": "created",
                            "limit": 10,
                        },
                    },
                    "input_mode": "none",
                    "output_contract": {"must_return": ["results", "count"]},
                },
            )
        )
        assert first["status"] == "waiting_work"
        work_result = kp.continue_run_work(first["cursor"], first["work"][0]["work_id"])
        assert work_result["outputs"]["count"] >= 1
        assert isinstance(work_result["outputs"]["results"], list)
        assert work_result["outputs"]["results"][0]["id"]
    finally:
        kp.close()


def test_action_summarize_uses_ctx_get_with_params_item_id(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        uri = "file:///tmp/state-actions-item-source.txt"
        kp.put(uri=uri, id="note:uri:summarize")

        def _updated_fetch(target_uri: str) -> Document:
            return Document(
                uri=target_uri,
                content="fresh uri content from ctx.get",
                content_type="text/plain",
                metadata={},
                tags=None,
            )

        kp._document_provider.fetch = _updated_fetch

        first = kp.continue_flow(
            _continue_with_single_step(
                "act-summarize-uri-1",
                {
                    "kind": "summarize",
                    "runner": {
                        "type": "action.summarize",
                        "params": {"item_id": "note:uri:summarize"},
                    },
                    "input_mode": "none",
                    "output_contract": {"must_return": ["summary"]},
                },
            )
        )
        assert first["status"] == "waiting_work"
        work_id = first["work"][0]["work_id"]
        work_result = kp.continue_run_work(first["cursor"], work_id)
        assert work_result["outputs"]["summary"] == "fresh uri content from ctx.get"

        second = kp.continue_flow(
            {
                "request_id": "act-summarize-uri-2",
                "cursor": first["cursor"],
                "work_results": [work_result],
            }
        )
        assert second["status"] == "done"
        item = kp.get("note:uri:summarize")
        assert item is not None
        assert item.summary == "fresh uri content from ctx.get"
    finally:
        kp.close()


def test_action_tag_uses_constrained_specs(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        seen: dict[str, str] = {}

        def _mock_generate(system: str, user: str, max_tokens: int = 2048) -> str:
            seen["system"] = system
            seen["user"] = user
            del max_tokens
            return "1: act=commitment(0.95) status=open(0.91)"

        mock_providers["summarization"].generate = _mock_generate

        kp._put_direct(content="Act taxonomy", id=".tag/act", tags={"_constrained": "true"}, summary="Act taxonomy")
        kp._put_direct(content="commitment", id=".tag/act/commitment", summary="Promise to do something")
        kp._put_direct(content="request", id=".tag/act/request", summary="Ask another party to act")
        kp._put_direct(content="Status taxonomy", id=".tag/status", tags={"_constrained": "true"}, summary="Status taxonomy")
        kp._put_direct(content="open", id=".tag/status/open", summary="Still pending")
        kp._put_direct(content="fulfilled", id=".tag/status/fulfilled", summary="Completed")
        doc_coll = kp._resolve_doc_collection()
        for parent_id in (".tag/act", ".tag/status"):
            rec = kp._document_store.get(doc_coll, parent_id)
            assert rec is not None
            tags = dict(rec.tags)
            tags["_constrained"] = "true"
            kp._document_store.update_tags(doc_coll, parent_id, tags)

        note_content = "I will ship the action runtime tomorrow."
        kp.put(content=note_content, id="note:action:tag", summary="placeholder")

        first = kp.continue_flow(
            _continue_with_single_step(
                "act-tag-1",
                {
                    "kind": "tag",
                    "runner": {"type": "action.tag"},
                    "input_mode": "note_content",
                    "output_contract": {"must_return": ["tags"]},
                },
                params={"id": "note:action:tag", "content": note_content},
            )
        )
        assert first["status"] == "waiting_work"
        work_id = first["work"][0]["work_id"]
        work_result = kp.continue_run_work(first["cursor"], work_id)
        assert "Tag: `act`" in seen.get("system", "")
        assert "Tag: `status`" in seen.get("system", "")
        assert "Classify these fragments" in seen.get("user", "")
        assert work_result["outputs"]["tags"] == {"act": "commitment", "status": "open"}

        second = kp.continue_flow(
            {
                "request_id": "act-tag-2",
                "cursor": first["cursor"],
                "work_results": [work_result],
            }
        )
        assert second["status"] == "done"
        item = kp.get("note:action:tag")
        assert item is not None
        assert item.tags.get("act") == "commitment"
        assert item.tags.get("status") == "open"
    finally:
        kp.close()


def test_action_traverse_uses_related_groups(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="source", id="note:source", tags={"topic": "actions", "role": "seed"}, summary="source")
        kp.put(content="related", id="note:related", tags={"topic": "actions"}, summary="related")
        kp.put(content="other", id="note:other", tags={"topic": "other"}, summary="other")

        first = kp.continue_flow(
            _continue_with_single_step(
                "act-traverse-1",
                {
                    "kind": "traverse",
                    "runner": {
                        "type": "action.traverse",
                        "params": {
                            "items": [{"id": "note:source"}],
                            "limit": 5,
                        },
                    },
                    "input_mode": "none",
                    "output_contract": {"must_return": ["groups", "count"]},
                },
            )
        )
        assert first["status"] == "waiting_work"
        work_result = kp.continue_run_work(first["cursor"], first["work"][0]["work_id"])
        groups = work_result["outputs"]["groups"]
        assert "note:source" in groups
        group_ids = {row["id"] for row in groups["note:source"]}
        assert "note:source" not in group_ids
        assert "note:related" in group_ids
    finally:
        kp.close()
