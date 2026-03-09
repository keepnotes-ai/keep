"""Tests for system doc validation."""

import pytest

from keep.validate import Diagnostic, ValidationResult, validate_system_doc


# ---------------------------------------------------------------------------
# Diagnostic / ValidationResult basics
# ---------------------------------------------------------------------------


class TestDiagnosticBasics:
    def test_str_with_location(self):
        d = Diagnostic("error", "bad syntax", "line 3")
        assert "[error] (line 3) bad syntax" == str(d)

    def test_str_without_location(self):
        d = Diagnostic("warning", "empty section")
        assert "[warning] empty section" == str(d)

    def test_result_ok_when_no_errors(self):
        r = ValidationResult("x", "tag", [Diagnostic("warning", "w")])
        assert r.ok

    def test_result_not_ok_when_errors(self):
        r = ValidationResult("x", "tag", [Diagnostic("error", "e")])
        assert not r.ok

    def test_result_errors_and_warnings(self):
        r = ValidationResult("x", "tag", [
            Diagnostic("error", "e1"),
            Diagnostic("warning", "w1"),
            Diagnostic("error", "e2"),
        ])
        assert len(r.errors) == 2
        assert len(r.warnings) == 1


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


class TestDispatcher:
    def test_unknown_prefix(self):
        r = validate_system_doc(".foo/bar", "content")
        assert r.doc_type == "unknown"
        assert r.ok

    def test_tag_dispatch(self):
        r = validate_system_doc(".tag/act", "A tag spec")
        assert r.doc_type == "tag"

    def test_meta_dispatch(self):
        r = validate_system_doc(".meta/related", "topic=")
        assert r.doc_type == "meta"

    def test_prompt_dispatch(self):
        r = validate_system_doc(
            ".prompt/analyze/default",
            "## Prompt\nAnalyze this",
        )
        assert r.doc_type == "prompt"


# ---------------------------------------------------------------------------
# .tag/* validation
# ---------------------------------------------------------------------------


class TestTagValidation:
    def test_valid_parent_unconstrained(self):
        r = validate_system_doc(".tag/topic", "Topic classification")
        assert r.ok
        assert len(r.errors) == 0

    def test_valid_parent_constrained_with_prompt(self):
        content = "Tag for actions.\n\n## Prompt\nClassify the speech act."
        r = validate_system_doc(".tag/act", content, {"_constrained": "true"})
        assert r.ok

    def test_constrained_missing_prompt_warns(self):
        r = validate_system_doc(
            ".tag/act",
            "A tag spec with no prompt section",
            {"_constrained": "true"},
        )
        assert r.ok  # warning, not error
        assert any("## Prompt" in d.message for d in r.warnings)

    def test_constrained_empty_prompt_warns(self):
        r = validate_system_doc(
            ".tag/act",
            "Desc.\n\n## Prompt\n\n## Other",
            {"_constrained": "true"},
        )
        assert any("## Prompt" in d.message for d in r.warnings)

    def test_valid_value_doc(self):
        r = validate_system_doc(".tag/act/commitment", "A binding promise")
        assert r.ok

    def test_value_doc_empty_prompt_warns(self):
        r = validate_system_doc(".tag/act/commitment", "Desc.\n\n## Prompt\n")
        assert any("empty" in d.message.lower() for d in r.warnings)

    def test_empty_content_warns(self):
        r = validate_system_doc(".tag/act", "")
        assert any("empty" in d.message.lower() for d in r.warnings)

    def test_too_deep_id_errors(self):
        r = validate_system_doc(".tag/a/b/c", "content")
        assert not r.ok
        assert any("too deep" in d.message for d in r.errors)

    def test_invalid_key_errors(self):
        r = validate_system_doc(".tag/123bad", "content")
        assert not r.ok

    def test_missing_key_errors(self):
        r = validate_system_doc(".tag/", "content")
        assert not r.ok

    def test_inverse_valid(self):
        r = validate_system_doc(".tag/project", "Projects", {"_inverse": "member_of"})
        assert r.ok

    def test_inverse_empty_errors(self):
        r = validate_system_doc(".tag/project", "Projects", {"_inverse": ""})
        assert not r.ok

    def test_inverse_bad_format_warns(self):
        r = validate_system_doc(".tag/project", "Projects", {"_inverse": "has spaces"})
        assert any("_inverse" in d.message for d in r.warnings)

    def test_singular_wrong_value_warns(self):
        r = validate_system_doc(".tag/act", "Act tag", {"_singular": "false"})
        assert any("_singular" in d.message for d in r.warnings)

    def test_constrained_wrong_value_warns(self):
        r = validate_system_doc(".tag/act", "Act tag", {"_constrained": "false"})
        assert any("_constrained" in d.message for d in r.warnings)


# ---------------------------------------------------------------------------
# .meta/* validation
# ---------------------------------------------------------------------------


class TestMetaValidation:
    def test_valid_query_lines(self):
        r = validate_system_doc(".meta/related", "topic=auth\nstatus=open")
        assert r.ok
        assert len(r.diagnostics) == 0

    def test_valid_context_key(self):
        r = validate_system_doc(".meta/related", "topic=")
        assert r.ok

    def test_valid_prereq(self):
        r = validate_system_doc(".meta/related", "topic=*")
        assert r.ok

    def test_valid_compound_query(self):
        r = validate_system_doc(".meta/related", "topic=auth status=open")
        assert r.ok

    def test_empty_content_errors(self):
        r = validate_system_doc(".meta/related", "")
        assert not r.ok

    def test_no_valid_rules_warns(self):
        r = validate_system_doc(".meta/related", "# Just a comment\n---\n")
        assert any("no valid" in d.message for d in r.warnings)

    def test_prose_lines_not_warned(self):
        r = validate_system_doc(
            ".meta/related",
            "This is prose description.\ntopic=auth\nMore prose.\nstatus=open",
        )
        assert r.ok
        assert len(r.warnings) == 0

    def test_malformed_rule_warns(self):
        r = validate_system_doc(
            ".meta/related",
            "topic=auth\nbad =value\nstatus=open",
        )
        assert r.ok  # warnings, not errors
        assert any("malformed" in d.message for d in r.warnings)

    def test_missing_name_errors(self):
        r = validate_system_doc(".meta/", "topic=auth")
        assert not r.ok

    def test_markdown_headers_skipped(self):
        r = validate_system_doc(
            ".meta/related",
            "# Description\n\ntopic=auth\n",
        )
        assert r.ok
        assert len(r.warnings) == 0

    def test_mixed_rules(self):
        content = "project=*\ntopic=\nstatus=open act=commitment"
        r = validate_system_doc(".meta/related", content)
        assert r.ok
        assert len(r.diagnostics) == 0


# ---------------------------------------------------------------------------
# .prompt/* validation
# ---------------------------------------------------------------------------


class TestPromptValidation:
    def test_valid_prompt(self):
        r = validate_system_doc(
            ".prompt/analyze/default",
            "## Prompt\nAnalyze the content for themes.",
        )
        assert r.ok

    def test_valid_prompt_with_match_rules(self):
        content = "topic=auth\n\n## Prompt\nAnalyze authentication patterns."
        r = validate_system_doc(".prompt/analyze/auth", content)
        assert r.ok

    def test_missing_prompt_section_errors(self):
        r = validate_system_doc(
            ".prompt/analyze/default",
            "This has no prompt section.",
        )
        assert not r.ok
        assert any("## Prompt" in d.message for d in r.errors)

    def test_empty_prompt_section_errors(self):
        r = validate_system_doc(
            ".prompt/analyze/default",
            "## Prompt\n\n",
        )
        assert not r.ok
        assert any("empty" in d.message.lower() for d in r.errors)

    def test_empty_content_errors(self):
        r = validate_system_doc(".prompt/analyze/default", "")
        assert not r.ok

    def test_bad_id_format_errors(self):
        r = validate_system_doc(".prompt/analyze", "## Prompt\nText")
        assert not r.ok

    def test_unknown_prefix_info(self):
        r = validate_system_doc(
            ".prompt/custom/default",
            "## Prompt\nCustom prompt text.",
        )
        assert r.ok  # info, not error
        assert any("not a known" in d.message for d in r.diagnostics)

    def test_prose_before_prompt_not_warned(self):
        content = "Description of this prompt.\n\n## Prompt\nPrompt text."
        r = validate_system_doc(".prompt/analyze/default", content)
        assert r.ok
        assert len(r.warnings) == 0

    def test_malformed_match_rule_warns(self):
        content = "bad =rule\n\n## Prompt\nPrompt text."
        r = validate_system_doc(".prompt/analyze/default", content)
        assert r.ok  # warning, not error
        assert any("malformed" in d.message for d in r.warnings)

    def test_valid_match_rules_no_warnings(self):
        content = "topic=auth\nstatus=open\n\n## Prompt\nPrompt text."
        r = validate_system_doc(".prompt/analyze/auth", content)
        assert r.ok
        assert len(r.warnings) == 0

    def test_context_match_in_prompt(self):
        content = "topic=\n\n## Prompt\nPrompt with context match."
        r = validate_system_doc(".prompt/analyze/default", content)
        assert r.ok

    def test_prereq_in_prompt(self):
        content = "topic=*\n\n## Prompt\nPrompt with prerequisite."
        r = validate_system_doc(".prompt/analyze/default", content)
        assert r.ok


# ---------------------------------------------------------------------------
# .state/* validation
# ---------------------------------------------------------------------------

VALID_STATE_DOC = """\
match: all
rules:
  - when: "item.content_length > 100 && !item.has_summary"
    id: summary
    do: summarize
  - when: "!item.is_system_note"
    id: tagged
    do: tag
post:
  - return: done
"""

VALID_SEQUENCE_DOC = """\
match: sequence
rules:
  - id: search
    do: find
    with: { query: "{params.query}", limit: 5 }
  - when: "search.margin > params.margin_high"
    return: done
  - then: query-explore
"""


class TestStateDocValidation:
    def test_valid_match_all(self):
        r = validate_system_doc(".state/after-write", VALID_STATE_DOC)
        assert r.doc_type == "state"
        assert r.ok

    def test_valid_match_sequence(self):
        r = validate_system_doc(".state/query-resolve", VALID_SEQUENCE_DOC)
        assert r.ok

    def test_empty_content_errors(self):
        r = validate_system_doc(".state/test", "")
        assert not r.ok

    def test_invalid_yaml_errors(self):
        r = validate_system_doc(".state/test", "{ bad yaml :::")
        assert not r.ok
        assert any("YAML" in d.message for d in r.errors)

    def test_not_a_mapping_errors(self):
        r = validate_system_doc(".state/test", "- just a list")
        assert not r.ok
        assert any("mapping" in d.message for d in r.errors)

    def test_missing_rules_errors(self):
        r = validate_system_doc(".state/test", "match: sequence")
        assert not r.ok
        assert any("rules must be a list" in d.message for d in r.errors)

    def test_invalid_match_errors(self):
        content = "match: invalid\nrules:\n  - return: done"
        r = validate_system_doc(".state/test", content)
        assert any("match must be" in d.message for d in r.errors)

    def test_empty_rules_warns(self):
        content = "rules: []"
        r = validate_system_doc(".state/test", content)
        assert r.ok  # warning, not error
        assert any("empty" in d.message for d in r.warnings)

    def test_non_dict_rule_errors(self):
        content = "rules:\n  - just a string"
        r = validate_system_doc(".state/test", content)
        assert not r.ok
        assert any("must be a mapping" in d.message for d in r.errors)

    def test_duplicate_rule_id_errors(self):
        content = """\
rules:
  - id: search
    do: find
    with: { query: "test" }
  - id: search
    do: find
    with: { query: "test2" }
"""
        r = validate_system_doc(".state/test", content)
        assert any("duplicate" in d.message for d in r.errors)

    def test_unknown_action_warns(self):
        content = """\
rules:
  - do: nonexistent_action
"""
        r = validate_system_doc(".state/test", content)
        assert any("unknown action" in d.message for d in r.warnings)

    def test_known_action_no_warning(self):
        content = """\
rules:
  - do: find
    with: { query: "test" }
"""
        r = validate_system_doc(".state/test", content)
        assert not any("unknown action" in d.message for d in r.diagnostics)

    def test_invalid_return_status_errors(self):
        content = """\
rules:
  - return: invalid_status
"""
        r = validate_system_doc(".state/test", content)
        assert any("return status" in d.message for d in r.errors)

    def test_valid_return_statuses(self):
        for status in ("done", "error", "stopped"):
            content = f"rules:\n  - return: {status}"
            r = validate_system_doc(".state/test", content)
            assert not any("return status" in d.message for d in r.errors), f"failed for {status}"

    def test_return_dict_form(self):
        content = """\
rules:
  - return:
      status: stopped
      with:
        reason: "budget"
"""
        r = validate_system_doc(".state/test", content)
        assert r.ok

    def test_then_string_form(self):
        content = """\
rules:
  - then: query-explore
"""
        r = validate_system_doc(".state/test", content)
        assert r.ok

    def test_then_dict_form(self):
        content = """\
rules:
  - then:
      state: query-branch
      with:
        facets: "{search.top_facets}"
"""
        r = validate_system_doc(".state/test", content)
        assert r.ok

    def test_then_missing_state_errors(self):
        content = """\
rules:
  - then:
      with:
        facets: "test"
"""
        r = validate_system_doc(".state/test", content)
        assert any("then.state" in d.message for d in r.errors)

    def test_post_with_sequence_warns(self):
        content = """\
match: sequence
rules:
  - return: done
post:
  - return: done
"""
        r = validate_system_doc(".state/test", content)
        assert any("post block" in d.message for d in r.warnings)

    def test_post_with_all_ok(self):
        content = """\
match: all
rules:
  - do: summarize
post:
  - return: done
"""
        r = validate_system_doc(".state/test", content)
        assert not any("post block" in d.message for d in r.warnings)

    def test_return_and_then_warns(self):
        content = """\
rules:
  - return: done
    then: other-state
"""
        r = validate_system_doc(".state/test", content)
        assert any("both return and then" in d.message for d in r.warnings)

    def test_rule_with_no_fields_warns(self):
        content = """\
rules:
  - id: empty_rule
"""
        r = validate_system_doc(".state/test", content)
        assert any("no do, then, return, or when" in d.message for d in r.warnings)

    def test_empty_when_errors(self):
        content = """\
rules:
  - when: ""
    return: done
"""
        r = validate_system_doc(".state/test", content)
        assert any("when must be" in d.message for d in r.errors)

    def test_empty_do_errors(self):
        content = """\
rules:
  - do: ""
"""
        r = validate_system_doc(".state/test", content)
        assert any("do must be" in d.message for d in r.errors)

    def test_with_not_dict_errors(self):
        content = """\
rules:
  - do: find
    with: "not a dict"
"""
        r = validate_system_doc(".state/test", content)
        assert any("with must be a mapping" in d.message for d in r.errors)

    def test_cel_compilation_error(self):
        content = """\
rules:
  - when: "this is not valid CEL @@!!"
    return: done
"""
        r = validate_system_doc(".state/test", content)
        assert not r.ok
        assert any("compile" in d.message.lower() or "compilation" in d.message.lower()
                    for d in r.errors)

    def test_template_refs_checked(self):
        content = """\
rules:
  - do: find
    with:
      query: "{params.query}"
      limit: "{params.limit}"
"""
        r = validate_system_doc(".state/test", content)
        # Valid template refs should not produce warnings about templates
        assert not any("template" in d.message.lower() for d in r.warnings)

    def test_missing_name_errors(self):
        r = validate_system_doc(".state/", "rules:\n  - return: done")
        assert not r.ok

    def test_dispatcher_routes_state(self):
        r = validate_system_doc(".state/test", "rules:\n  - return: done")
        assert r.doc_type == "state"
