"""Regression tests for store-backed prompt and state doc authority."""

from keep.api import Keeper
from keep.flow_env import LocalFlowEnvironment
from keep.state_doc_runtime import make_action_runner


def _ensure_system_docs(kp: Keeper) -> None:
    """Trigger system doc migration in temp-store tests."""
    kp.put("migration trigger", id="_prompt-test-trigger")
    kp.delete("_prompt-test-trigger")


def test_render_prompt_requires_state_for_dynamic_prompt(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    _ensure_system_docs(kp)

    kp.put(
        "# Test\n\n## Prompt\nHello {get}",
        id=".prompt/agent/test-dynamic",
        tags={"category": "system", "context": "prompt"},
    )

    result = kp.run_flow_command("prompt", params={"name": "test-dynamic"})

    assert result.status == "error"
    assert "no state tag" in str(result.data.get("error", "")).lower()


def test_prompt_list_bootstraps_on_fresh_store(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)

    result = kp.run_flow_command("prompt", params={"list": True})

    assert result.status == "done"
    prompts = result.data.get("prompts", [])
    assert any(prompt.get("name") == "reflect" for prompt in prompts)


def test_summarize_action_errors_when_default_prompt_is_broken(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    _ensure_system_docs(kp)
    kp.put("Content to summarize", id="doc-1")
    doc_coll = kp._resolve_doc_collection()
    for rec in kp._document_store.query_by_id_prefix(doc_coll, ".prompt/summarize/"):
        kp._document_store.delete(doc_coll, rec.id)

    runner = make_action_runner(LocalFlowEnvironment(kp), writable=True)

    try:
        runner("summarize", {"item_id": "doc-1", "force": True})
        assert False, "summarize should fail when the default prompt doc is broken"
    except ValueError as exc:
        assert "missing prompt doc for summarize" in str(exc).lower()


def test_analyze_action_errors_when_default_prompt_is_broken(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    _ensure_system_docs(kp)
    kp.put("Analyze this content into parts.", id="doc-2")
    doc_coll = kp._resolve_doc_collection()
    for rec in kp._document_store.query_by_id_prefix(doc_coll, ".prompt/analyze/"):
        kp._document_store.delete(doc_coll, rec.id)

    runner = make_action_runner(LocalFlowEnvironment(kp), writable=True)

    try:
        runner("analyze", {"item_id": "doc-2", "force": True})
        assert False, "analyze should fail when the default prompt doc is broken"
    except ValueError as exc:
        assert "missing prompt doc for analyze" in str(exc).lower()
