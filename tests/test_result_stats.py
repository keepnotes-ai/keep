"""Tests for result statistics enrichment."""

from keep.result_stats import enrich_find_output


def _result(id, score=None, tags=None):
    return {"id": id, "summary": "", "tags": tags or {}, "score": score}


class TestMargin:
    def test_dominant_result(self):
        out = enrich_find_output({
            "results": [_result("a", 0.95), _result("b", 0.30)],
            "count": 2,
        })
        assert out["margin"] > 0.5

    def test_tied_results(self):
        out = enrich_find_output({
            "results": [_result("a", 0.50), _result("b", 0.49)],
            "count": 2,
        })
        assert out["margin"] < 0.1

    def test_single_result(self):
        out = enrich_find_output({
            "results": [_result("a", 0.80)],
            "count": 1,
        })
        assert out["margin"] == 1.0

    def test_no_results(self):
        out = enrich_find_output({"results": [], "count": 0})
        assert out["margin"] is None

    def test_no_scores_list_mode(self):
        out = enrich_find_output({
            "results": [_result("a"), _result("b")],
            "count": 2,
        })
        assert out["margin"] is None


class TestEntropy:
    def test_uniform_scores(self):
        out = enrich_find_output({
            "results": [_result("a", 0.5), _result("b", 0.5), _result("c", 0.5)],
            "count": 3,
        })
        assert out["entropy"] > 0.9  # near-uniform → high entropy

    def test_peaked_scores(self):
        out = enrich_find_output({
            "results": [_result("a", 0.99), _result("b", 0.01), _result("c", 0.01)],
            "count": 3,
        })
        assert out["entropy"] < 0.5  # peaked → low entropy

    def test_single_result(self):
        out = enrich_find_output({
            "results": [_result("a", 0.80)],
            "count": 1,
        })
        assert out["entropy"] == 0.0

    def test_no_scores(self):
        out = enrich_find_output({
            "results": [_result("a"), _result("b")],
            "count": 2,
        })
        assert out["entropy"] is None


class TestLineage:
    def test_no_lineage_tags(self):
        out = enrich_find_output({
            "results": [_result("a", 0.9, {"topic": "x"}), _result("b", 0.8)],
            "count": 2,
        })
        assert out["lineage_strong"] == 0.0
        assert out["dominant_lineage_tags"] is None

    def test_strong_lineage(self):
        out = enrich_find_output({
            "results": [
                _result("a", 0.9, {"_base_id": "%parent"}),
                _result("b", 0.8, {"_base_id": "%parent"}),
                _result("c", 0.7, {"_base_id": "%parent"}),
                _result("d", 0.6, {"topic": "other"}),
            ],
            "count": 4,
        })
        assert out["lineage_strong"] == 1.0  # all lineage items share same root
        assert out["dominant_lineage_tags"]["_base_id"] == "%parent"

    def test_mixed_lineage(self):
        out = enrich_find_output({
            "results": [
                _result("a", 0.9, {"_base_id": "%parent1"}),
                _result("b", 0.8, {"_base_id": "%parent2"}),
                _result("c", 0.7, {"_base_id": "%parent1"}),
                _result("d", 0.6, {"_base_id": "%parent2"}),
            ],
            "count": 4,
        })
        assert out["lineage_strong"] == 0.5  # evenly split

    def test_version_of_key(self):
        out = enrich_find_output({
            "results": [
                _result("a", 0.9, {"_version_of": "%doc"}),
                _result("b", 0.8, {"_version_of": "%doc"}),
            ],
            "count": 2,
        })
        assert out["lineage_strong"] == 1.0


class TestTopFacetTags:
    def test_common_facet(self):
        out = enrich_find_output({
            "results": [
                _result("a", 0.9, {"topic": "auth"}),
                _result("b", 0.8, {"topic": "auth"}),
                _result("c", 0.7, {"topic": "db"}),
            ],
            "count": 3,
        })
        assert len(out["top_facet_tags"]) >= 1
        assert out["top_facet_tags"][0] == {"topic": "auth"}

    def test_system_tags_excluded(self):
        out = enrich_find_output({
            "results": [
                _result("a", 0.9, {"_source": "inline", "topic": "x"}),
                _result("b", 0.8, {"_source": "inline", "topic": "x"}),
            ],
            "count": 2,
        })
        tags = out["top_facet_tags"]
        assert not any("_source" in t for t in tags)

    def test_minimum_count_threshold(self):
        out = enrich_find_output({
            "results": [
                _result("a", 0.9, {"topic": "one"}),
                _result("b", 0.8, {"topic": "two"}),
                _result("c", 0.7, {"topic": "three"}),
            ],
            "count": 3,
        })
        # Each appears only once → below threshold (count < 2)
        assert out["top_facet_tags"] == []

    def test_empty_results(self):
        out = enrich_find_output({"results": [], "count": 0})
        assert out["top_facet_tags"] == []

    def test_list_tag_values(self):
        out = enrich_find_output({
            "results": [
                _result("a", 0.9, {"act": ["commitment", "request"]}),
                _result("b", 0.8, {"act": ["commitment"]}),
            ],
            "count": 2,
        })
        tags = out["top_facet_tags"]
        assert {"act": "commitment"} in tags


class TestEnrichPreservesOutput:
    def test_original_fields_preserved(self):
        out = enrich_find_output({
            "results": [_result("a", 0.9)],
            "count": 1,
        })
        assert out["results"][0]["id"] == "a"
        assert out["count"] == 1

    def test_no_results_key(self):
        out = enrich_find_output({"count": 0})
        assert "margin" not in out  # passthrough when no results list

    def test_non_list_results(self):
        out = enrich_find_output({"results": "not a list", "count": 0})
        assert "margin" not in out
