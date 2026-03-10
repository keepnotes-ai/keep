"""Tests for state-doc loader, compiler, and evaluator."""

import pytest

from keep.state_doc import (
    CompiledRule,
    EvalResult,
    StateDoc,
    evaluate_state_doc,
    parse_state_doc,
)

# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


class TestParseStateDoc:
    def test_minimal_sequence(self):
        body = """
match: sequence
rules:
  - do: find
    with: { query: "test" }
"""
        doc = parse_state_doc("test", body)
        assert doc.name == "test"
        assert doc.match == "sequence"
        assert len(doc.rules) == 1
        assert doc.rules[0].do == "find"
        assert doc.rules[0].with_params == {"query": "test"}

    def test_match_all_with_post(self):
        body = """
match: all
rules:
  - id: summary
    do: summarize
    with: { item_id: "{params.item_id}" }
  - id: tags
    do: tag
    with: { item_id: "{params.item_id}" }
post:
  - return: done
"""
        doc = parse_state_doc("after-write", body)
        assert doc.match == "all"
        assert len(doc.rules) == 2
        assert doc.rules[0].id == "summary"
        assert doc.rules[1].id == "tags"
        assert len(doc.post) == 1
        assert doc.post[0].return_status == "done"

    def test_when_predicate_compiled(self):
        body = """
match: sequence
rules:
  - when: "item.content_length > 100"
    do: summarize
"""
        doc = parse_state_doc("test", body)
        assert doc.rules[0].when is not None
        assert doc.rules[0].when_source == "item.content_length > 100"

    def test_then_transition(self):
        body = """
match: sequence
rules:
  - then: query-broaden
"""
        doc = parse_state_doc("test", body)
        assert doc.rules[0].then == "query-broaden"

    def test_then_with_data(self):
        body = """
match: sequence
rules:
  - then:
      state: query-broaden
      with:
        depth: 2
"""
        doc = parse_state_doc("test", body)
        assert isinstance(doc.rules[0].then, dict)
        assert doc.rules[0].then["state"] == "query-broaden"

    def test_return_terminal(self):
        body = """
match: sequence
rules:
  - return: done
"""
        doc = parse_state_doc("test", body)
        assert doc.rules[0].return_status == "done"

    def test_invalid_match_strategy(self):
        body = """
match: invalid
rules:
  - do: find
"""
        with pytest.raises(ValueError, match="match must be"):
            parse_state_doc("test", body)

    def test_default_match_is_sequence(self):
        body = """
rules:
  - do: find
"""
        doc = parse_state_doc("test", body)
        assert doc.match == "sequence"

    def test_unconditional_rule(self):
        body = """
match: sequence
rules:
  - id: search
    do: find
    with: { query: "test" }
"""
        doc = parse_state_doc("test", body)
        assert doc.rules[0].when is None  # unconditional


# ---------------------------------------------------------------------------
# Sequence evaluation
# ---------------------------------------------------------------------------


class TestEvalSequence:
    def test_unconditional_action(self):
        body = """
match: sequence
rules:
  - id: search
    do: find
    with: { query: "test" }
  - return: done
"""
        doc = parse_state_doc("test", body)
        result = evaluate_state_doc(doc, {})
        assert len(result.actions) == 1
        assert result.actions[0]["action"] == "find"
        assert result.actions[0]["params"] == {"query": "test"}
        # No run_action callback, so terminal comes from return rule
        # But sequence evaluates the return rule only if find had no callback
        assert result.terminal == "done"

    def test_predicate_gates_action(self):
        body = """
match: sequence
rules:
  - when: "item.content_length > 100"
    do: summarize
  - return: done
"""
        doc = parse_state_doc("test", body)

        # Item too short — summarize should not fire
        result = evaluate_state_doc(doc, {"item": {"content_length": 50}})
        assert len(result.actions) == 0
        assert result.terminal == "done"

        # Item long enough — summarize fires
        result = evaluate_state_doc(doc, {"item": {"content_length": 200}})
        assert len(result.actions) == 1
        assert result.actions[0]["action"] == "summarize"

    def test_sequence_short_circuits_on_return(self):
        body = """
match: sequence
rules:
  - when: "x > 10"
    return: done
  - do: find
"""
        doc = parse_state_doc("test", body)
        result = evaluate_state_doc(doc, {"x": 20})
        assert result.terminal == "done"
        assert len(result.actions) == 0  # find never reached

    def test_sequence_short_circuits_on_transition(self):
        body = """
match: sequence
rules:
  - when: "x > 10"
    then: other-state
  - do: find
"""
        doc = parse_state_doc("test", body)
        result = evaluate_state_doc(doc, {"x": 20})
        assert result.transition == "other-state"
        assert result.terminal is None
        assert len(result.actions) == 0

    def test_output_binding_in_sequence(self):
        body = """
match: sequence
rules:
  - id: search
    do: find
    with: { query: "test" }
  - when: "search.margin > 0.5"
    return: done
  - return: stopped
"""
        doc = parse_state_doc("test", body)

        def mock_action(name, params):
            if name == "find":
                return {"results": [], "margin": 0.8, "count": 0}
            return {}

        result = evaluate_state_doc(doc, {}, run_action=mock_action)
        assert result.terminal == "done"  # margin 0.8 > 0.5
        assert "search" in result.bindings
        assert result.bindings["search"]["margin"] == 0.8

    def test_output_binding_low_margin_falls_through(self):
        body = """
match: sequence
rules:
  - id: search
    do: find
    with: { query: "test" }
  - when: "search.margin > 0.5"
    return: done
  - return: stopped
"""
        doc = parse_state_doc("test", body)

        def mock_action(name, params):
            return {"results": [], "margin": 0.1, "count": 0}

        result = evaluate_state_doc(doc, {}, run_action=mock_action)
        assert result.terminal == "stopped"  # margin 0.1 < 0.5, fell through

    def test_template_resolution_in_with(self):
        body = """
match: sequence
rules:
  - do: find
    with: { query: "{params.query}", limit: 5 }
"""
        doc = parse_state_doc("test", body)
        result = evaluate_state_doc(doc, {"params": {"query": "auth patterns"}})
        assert result.actions[0]["params"]["query"] == "auth patterns"
        assert result.actions[0]["params"]["limit"] == 5

    def test_transition_with_data_resolution(self):
        body = """
match: sequence
rules:
  - id: search
    do: find
    with: { query: "test" }
  - then:
      state: query-broaden
      with:
        original_count: "{search.count}"
"""
        doc = parse_state_doc("test", body)

        def mock_action(name, params):
            return {"results": [], "count": 3}

        result = evaluate_state_doc(doc, {}, run_action=mock_action)
        assert isinstance(result.transition, dict)
        assert result.transition["with"]["original_count"] == 3

    def test_default_terminal_when_no_return(self):
        body = """
match: sequence
rules:
  - do: find
    with: { query: "test" }
"""
        doc = parse_state_doc("test", body)
        result = evaluate_state_doc(doc, {})
        assert result.terminal == "done"  # implicit

    def test_failed_action_leaves_binding_unset(self):
        body = """
match: sequence
rules:
  - id: broken
    do: summarize
  - when: "broken.summary != ''"
    return: done
  - return: error
"""
        doc = parse_state_doc("test", body)

        def failing_action(name, params):
            raise RuntimeError("provider unavailable")

        result = evaluate_state_doc(doc, {}, run_action=failing_action)
        assert result.terminal == "error"
        assert "broken" not in result.bindings


# ---------------------------------------------------------------------------
# Match-all evaluation
# ---------------------------------------------------------------------------


class TestEvalAll:
    def test_all_matching_rules_fire(self):
        body = """
match: all
rules:
  - when: "item.needs_summary"
    id: summary
    do: summarize
    with: { item_id: "{params.item_id}" }
  - when: "item.needs_tags"
    id: tags
    do: tag
    with: { item_id: "{params.item_id}" }
  - when: "item.needs_ocr"
    do: ocr
post:
  - return: done
"""
        doc = parse_state_doc("test", body)
        ctx = {
            "item": {"needs_summary": True, "needs_tags": True, "needs_ocr": False},
            "params": {"item_id": "abc"},
        }
        result = evaluate_state_doc(doc, ctx)
        assert len(result.actions) == 2
        assert result.actions[0]["action"] == "summarize"
        assert result.actions[1]["action"] == "tag"
        assert result.terminal == "done"

    def test_post_block_checks_outputs(self):
        body = """
match: all
rules:
  - id: summary
    do: summarize
post:
  - when: "summary.text == ''"
    return: error
  - return: done
"""
        doc = parse_state_doc("test", body)

        def mock_action(name, params):
            return {"text": "A summary"}

        result = evaluate_state_doc(doc, {}, run_action=mock_action)
        assert result.terminal == "done"

    def test_no_post_defaults_to_done(self):
        body = """
match: all
rules:
  - do: summarize
"""
        doc = parse_state_doc("test", body)
        result = evaluate_state_doc(doc, {})
        assert result.terminal == "done"

    def test_no_matching_rules(self):
        body = """
match: all
rules:
  - when: "false"
    do: summarize
post:
  - return: done
"""
        doc = parse_state_doc("test", body)
        result = evaluate_state_doc(doc, {})
        assert len(result.actions) == 0
        assert result.terminal == "done"


# ---------------------------------------------------------------------------
# CEL predicate edge cases
# ---------------------------------------------------------------------------


class TestPredicates:
    def test_boolean_operators(self):
        body = """
match: sequence
rules:
  - when: "x > 5 && y < 10"
    return: done
  - return: error
"""
        doc = parse_state_doc("test", body)
        assert evaluate_state_doc(doc, {"x": 8, "y": 3}).terminal == "done"
        assert evaluate_state_doc(doc, {"x": 3, "y": 3}).terminal == "error"
        assert evaluate_state_doc(doc, {"x": 8, "y": 15}).terminal == "error"

    def test_negation(self):
        body = """
match: sequence
rules:
  - when: "!item.has_summary"
    do: summarize
  - return: done
"""
        doc = parse_state_doc("test", body)
        r1 = evaluate_state_doc(doc, {"item": {"has_summary": False}})
        assert len(r1.actions) == 1

        r2 = evaluate_state_doc(doc, {"item": {"has_summary": True}})
        assert len(r2.actions) == 0

    def test_string_equality(self):
        body = """
match: sequence
rules:
  - when: 'item.source == "uri"'
    do: ocr
  - return: done
"""
        doc = parse_state_doc("test", body)
        r1 = evaluate_state_doc(doc, {"item": {"source": "uri"}})
        assert len(r1.actions) == 1

        r2 = evaluate_state_doc(doc, {"item": {"source": "inline"}})
        assert len(r2.actions) == 0

    def test_has_macro(self):
        body = """
match: sequence
rules:
  - when: "has(item.tags.topic)"
    return: done
  - return: stopped
"""
        doc = parse_state_doc("test", body)
        r1 = evaluate_state_doc(doc, {"item": {"tags": {"topic": "auth"}}})
        assert r1.terminal == "done"

        r2 = evaluate_state_doc(doc, {"item": {"tags": {}}})
        assert r2.terminal == "stopped"

    def test_size_function(self):
        body = """
match: sequence
rules:
  - when: "size(results) > 0"
    return: done
  - return: stopped
"""
        doc = parse_state_doc("test", body)
        assert evaluate_state_doc(doc, {"results": [1, 2, 3]}).terminal == "done"
        assert evaluate_state_doc(doc, {"results": []}).terminal == "stopped"

    def test_in_operator(self):
        body = """
match: sequence
rules:
  - when: '"admin" in item.roles'
    return: done
  - return: stopped
"""
        doc = parse_state_doc("test", body)
        assert evaluate_state_doc(doc, {"item": {"roles": ["admin", "user"]}}).terminal == "done"
        assert evaluate_state_doc(doc, {"item": {"roles": ["user"]}}).terminal == "stopped"

    def test_predicate_error_treated_as_false(self):
        body = """
match: sequence
rules:
  - when: "nonexistent.field > 5"
    return: done
  - return: stopped
"""
        doc = parse_state_doc("test", body)
        # Missing variable should not crash, just return false
        assert evaluate_state_doc(doc, {}).terminal == "stopped"


# ---------------------------------------------------------------------------
# Integration: after-write state doc
# ---------------------------------------------------------------------------


class TestAfterWriteIntegration:
    """Test a realistic after-write state doc."""

    AFTER_WRITE = """
match: all
rules:
  - when: "item.content_length > params.max_summary_length && !item.has_summary"
    id: summary
    do: summarize
    with:
      item_id: "{params.item_id}"
      max_length: 500
  - when: "!item.is_system_note"
    id: tags
    do: tag
    with:
      item_id: "{params.item_id}"
  - when: 'item.source == "uri" && has(item.ocr_pages)'
    do: ocr
    with:
      item_id: "{params.item_id}"
post:
  - return: done
"""

    def test_long_text_triggers_summarize_and_tag(self):
        doc = parse_state_doc("after-write", self.AFTER_WRITE)
        ctx = {
            "item": {
                "content_length": 5000,
                "has_summary": False,
                "is_system_note": False,
                "source": "inline",
            },
            "params": {"item_id": "%abc123", "max_summary_length": 2000},
        }

        actions_run = []

        def mock_action(name, params):
            actions_run.append(name)
            if name == "summarize":
                return {"summary": "A summary"}
            if name == "tag":
                return {"tags": {"topic": "test"}}
            return {}

        result = evaluate_state_doc(doc, ctx, run_action=mock_action)
        assert "summarize" in actions_run
        assert "tag" in actions_run
        assert "ocr" not in actions_run
        assert result.terminal == "done"

    def test_short_text_skips_summarize(self):
        doc = parse_state_doc("after-write", self.AFTER_WRITE)
        ctx = {
            "item": {
                "content_length": 100,
                "has_summary": False,
                "is_system_note": False,
                "source": "inline",
            },
            "params": {"item_id": "%abc123", "max_summary_length": 2000},
        }
        result = evaluate_state_doc(doc, ctx)
        action_names = [a["action"] for a in result.actions]
        assert "summarize" not in action_names
        assert "tag" in action_names

    def test_system_note_skips_tagging(self):
        doc = parse_state_doc("after-write", self.AFTER_WRITE)
        ctx = {
            "item": {
                "content_length": 5000,
                "has_summary": False,
                "is_system_note": True,
                "source": "inline",
            },
            "params": {"item_id": ".meta/test", "max_summary_length": 2000},
        }
        result = evaluate_state_doc(doc, ctx)
        action_names = [a["action"] for a in result.actions]
        assert "summarize" in action_names
        assert "tag" not in action_names


# ---------------------------------------------------------------------------
# Regression tests for specific bugs
# ---------------------------------------------------------------------------


class TestBugfixes:
    def test_action_without_id_still_executes(self):
        """Bug #1: run_action was gated on rule.id, skipping id-less actions."""
        body = """
match: sequence
rules:
  - do: notify
    with: { message: "hello" }
  - return: done
"""
        doc = parse_state_doc("test", body)
        executed = []

        def mock_action(name, params):
            executed.append(name)
            return {}

        result = evaluate_state_doc(doc, {}, run_action=mock_action)
        assert "notify" in executed
        assert result.terminal == "done"

    def test_return_string_does_not_capture_sibling_with(self):
        """Bug #2: return: done + sibling with: incorrectly set return_with."""
        body = """
match: sequence
rules:
  - do: find
    with: { query: "test" }
    return: done
"""
        doc = parse_state_doc("test", body)
        # The rule has both do+with and return: done
        # return_with should be None (only dict-form return carries payload)
        assert doc.rules[0].return_with is None

    def test_post_block_resolves_then_with_templates(self):
        """Bug #3: post block then.with templates were not resolved."""
        body = """
match: all
rules:
  - id: search
    do: find
    with: { query: "test" }
post:
  - then:
      state: next
      with:
        count: "{search.count}"
"""
        doc = parse_state_doc("test", body)

        def mock_action(name, params):
            return {"results": [], "count": 7}

        result = evaluate_state_doc(doc, {}, run_action=mock_action)
        assert isinstance(result.transition, dict)
        assert result.transition["with"]["count"] == 7

    def test_non_dict_rule_warns(self, caplog):
        """Bug #4: non-dict entries in rules silently ignored."""
        import logging

        body = """
match: sequence
rules:
  - do: find
  - "this is not a rule"
  - do: tag
"""
        with caplog.at_level(logging.WARNING, logger="keep.state_doc"):
            doc = parse_state_doc("test", body)
        assert len(doc.rules) == 2
        assert "rules[1] is not a mapping" in caplog.text
