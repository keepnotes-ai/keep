"""
Shared pytest fixtures for keep tests.

Provides mock providers to avoid loading heavy ML models during testing.
"""

import hashlib
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from keep.types import SYSTEM_TAG_PREFIX, tag_values


class MockEmbeddingProvider:
    """
    Deterministic mock embedding provider for testing.

    Generates consistent embeddings based on text hash - no ML model loading.
    """

    dimension = 384
    model_name = "mock-model"

    def __init__(self):
        self.embed_calls = 0
        self.batch_calls = 0

    def embed(self, text: str) -> list[float]:
        """Generate deterministic embedding from text hash."""
        self.embed_calls += 1
        h = hashlib.md5(text.encode()).hexdigest()
        # Create 384-dim vector from hash (deterministic)
        embedding = []
        for i in range(0, 32, 2):
            val = int(h[i:i+2], 16) / 255.0
            embedding.append(val)
        # Pad to full dimension
        embedding = (embedding * 24)[:self.dimension]
        return embedding

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings for multiple texts."""
        self.batch_calls += 1
        return [self.embed(t) for t in texts]


class MockSummarizationProvider:
    """Mock summarization provider - returns truncated content."""

    def summarize(self, content: str, *, context: str | None = None) -> str:
        """Return first 200 chars as summary."""
        return content[:200] if len(content) > 200 else content


class MockDocumentProvider:
    """Mock document provider for URI fetching."""

    def fetch(self, uri: str) -> Any:
        """Return mock document."""
        mock_doc = MagicMock()
        mock_doc.content = f"Content for {uri}"
        mock_doc.content_type = "text/plain"
        mock_doc.tags = None
        return mock_doc


@pytest.fixture
def mock_embedding_provider():
    """Create a fresh MockEmbeddingProvider instance."""
    return MockEmbeddingProvider()


class MockChromaStore:
    """Mock ChromaStore to avoid loading real ChromaDB."""

    def __init__(self, store_path: Path, embedding_dimension: int = None):
        self._store_path = store_path
        self._embedding_dimension = embedding_dimension or 384
        self.embedding_dimension = self._embedding_dimension
        self._data: dict[str, dict] = {}  # collection -> {id -> record}
        self._tag_key_prefix = "_tk::"
        self._tag_value_prefix = "_tv::"

    def _tag_key_marker(self, key: str) -> str:
        return f"{self._tag_key_prefix}{key}"

    def _tag_value_marker(self, key: str, value: str) -> str:
        digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]
        return f"{self._tag_value_prefix}{key}::{digest}"

    def build_tag_where(self, tags: dict) -> dict | None:
        conditions: list[dict[str, Any]] = []
        for raw_key in tags:
            key = str(raw_key)
            values = tag_values(tags, raw_key)
            if not values:
                continue
            if key.startswith(SYSTEM_TAG_PREFIX):
                for value in values:
                    conditions.append({key: str(value)})
            else:
                key_cf = key.casefold()
                conditions.append({self._tag_key_marker(key_cf): True})
                for value in values:
                    conditions.append({self._tag_value_marker(key_cf, value): True})
        if not conditions:
            return None
        return conditions[0] if len(conditions) == 1 else {"$and": conditions}

    def _to_metadata(self, tags: dict) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        for raw_key in tags:
            key = str(raw_key)
            values = tag_values(tags, raw_key)
            if not values:
                continue
            if key.startswith(SYSTEM_TAG_PREFIX):
                metadata[key] = str(values[-1])
            else:
                key_cf = key.casefold()
                if len(values) == 1:
                    metadata[key_cf] = values[0]
                metadata[self._tag_key_marker(key_cf)] = True
                for value in values:
                    metadata[self._tag_value_marker(key_cf, value)] = True
        return metadata

    def reset_embedding_dimension(self, dimension: int) -> None:
        self._embedding_dimension = dimension
        self.embedding_dimension = dimension

    def upsert(self, collection: str, id: str, embedding: list[float],
               summary: str, tags: dict[str, str]) -> None:
        if collection not in self._data:
            self._data[collection] = {}
        self._data[collection][id] = {
            "summary": summary,
            "tags": tags,
            "metadata": self._to_metadata(tags),
            "embedding": embedding,
        }

    def get(self, collection: str, id: str):
        from keep.store import StoreResult
        if collection not in self._data or id not in self._data[collection]:
            return None
        rec = self._data[collection][id]
        return StoreResult(id=id, summary=rec["summary"], tags=rec["tags"])

    def exists(self, collection: str, id: str) -> bool:
        return collection in self._data and id in self._data[collection]

    def delete(self, collection: str, id: str, delete_versions: bool = True) -> bool:
        if collection in self._data and id in self._data[collection]:
            del self._data[collection][id]
            return True
        return False

    def _match_where(self, tags: dict, where: dict | None) -> bool:
        """Check if tags match a ChromaDB where clause."""
        if not where:
            return True
        conditions = {}
        if "$and" in where:
            for clause in where["$and"]:
                conditions.update(clause)
        else:
            conditions = where
        return all(tags.get(k) == v for k, v in conditions.items())

    def query_embedding(self, collection: str, embedding: list[float],
                       limit: int = 10, where: dict = None) -> list:
        from keep.store import StoreResult
        if collection not in self._data:
            return []
        results = []
        for id, rec in list(self._data[collection].items()):
            if self._match_where(rec.get("metadata", {}), where):
                results.append(StoreResult(
                    id=id, summary=rec["summary"], tags=rec["tags"], distance=0.1
                ))
            if len(results) >= limit:
                break
        return results

    def query_metadata(self, collection: str, where: dict, limit: int = 100, offset: int = 0) -> list:
        from keep.store import StoreResult
        if collection not in self._data:
            return []
        results = []
        for id, rec in list(self._data[collection].items()):
            if self._match_where(rec.get("metadata", {}), where):
                results.append(StoreResult(id=id, summary=rec["summary"], tags=rec["tags"]))
        return results[offset:offset + limit]

    def list_collections(self) -> list[str]:
        return list(self._data.keys()) or ["default"]

    def count(self, collection: str) -> int:
        return len(self._data.get(collection, {}))

    def get_embedding(self, collection: str, id: str) -> list[float] | None:
        if collection in self._data and id in self._data[collection]:
            return self._data[collection][id].get("embedding")
        return None

    def get_entries_full(self, collection: str, ids: list[str]) -> list[dict]:
        results = []
        if collection not in self._data:
            return results
        for id in ids:
            if id in self._data[collection]:
                rec = self._data[collection][id]
                results.append({
                    "id": id,
                    "embedding": rec.get("embedding"),
                    "summary": rec.get("summary"),
                    "tags": rec.get("tags", {}),
                })
        return results

    def upsert_batch(self, collection: str, ids: list[str],
                     embeddings: list[list[float]], summaries: list[str],
                     tags: list[dict[str, str]]) -> None:
        for i, id in enumerate(ids):
            self.upsert(collection, id, embeddings[i], summaries[i], tags[i])

    def upsert_version(self, collection: str, id: str, version: int,
                       embedding: list[float], summary: str,
                       tags: dict[str, str]) -> None:
        versioned_id = f"{id}@v{version}"
        self.upsert(collection, versioned_id, embedding, summary, tags)

    def upsert_part(self, collection: str, id: str, part_num: int,
                    embedding: list[float], summary: str,
                    tags: dict[str, str]) -> None:
        part_id = f"{id}@p{part_num}"
        part_tags = dict(tags)
        part_tags["_part_num"] = str(part_num)
        part_tags["_base_id"] = id
        self.upsert(collection, part_id, embedding, summary, part_tags)

    def delete_parts(self, collection: str, id: str) -> int:
        if collection not in self._data:
            return 0
        part_ids = [k for k in self._data[collection] if k.startswith(f"{id}@p")]
        for pid in part_ids:
            del self._data[collection][pid]
        return len(part_ids)

    def delete_entries(self, collection: str, ids: list[str]) -> None:
        if collection in self._data:
            for id in ids:
                self._data[collection].pop(id, None)

    def list_ids(self, collection: str) -> list[str]:
        return list(self._data.get(collection, {}).keys())

    def find_missing_ids(self, collection: str, ids: list[str]) -> set[str]:
        existing = set(self._data.get(collection, {}).keys())
        return {id for id in ids if id not in existing}

    def update_summary(self, collection: str, id: str, summary: str) -> bool:
        if collection in self._data and id in self._data[collection]:
            self._data[collection][id]["summary"] = summary
            return True
        return False

    def update_tags(self, collection: str, id: str, tags: dict) -> bool:
        if collection in self._data and id in self._data[collection]:
            self._data[collection][id]["tags"] = tags
            self._data[collection][id]["metadata"] = self._to_metadata(tags)
            return True
        return False

    def rewrite_tags(self, collection: str, id: str, tags: dict) -> bool:
        """Rewrite metadata without semantic changes (migration helper)."""
        return self.update_tags(collection, id, tags)

    def has_tag_markers(self, collection: str, id: str) -> bool:
        if collection not in self._data or id not in self._data[collection]:
            return False
        metadata = self._data[collection][id].get("metadata", {})
        return any(
            k.startswith(self._tag_key_prefix) or k.startswith(self._tag_value_prefix)
            for k in metadata
        )

    def delete_collection(self, name: str) -> bool:
        if name in self._data:
            del self._data[name]
            return True
        return False

    def close(self) -> None:
        self._data.clear()


class MockDocumentStore:
    """Mock DocumentStore to avoid loading real SQLite."""

    def __init__(self, db_path: Path):
        self._data: dict[str, dict] = {}  # collection -> {id -> record}
        self._parts: dict[str, list] = {}  # _parts:{collection}:{id} -> [PartInfo]

    def _make_record(self, collection: str, id: str, rec: dict) -> "DocumentRecord":
        from keep.document_store import DocumentRecord
        return DocumentRecord(
            id=id,
            collection=collection,
            summary=rec["summary"],
            tags=rec["tags"],
            created_at=rec["created_at"],
            updated_at=rec["updated_at"],
            content_hash=rec.get("content_hash"),
            content_hash_full=rec.get("content_hash_full"),
            accessed_at=rec.get("accessed_at", rec["updated_at"]),
        )

    def upsert(self, collection: str, id: str, summary: str, tags: dict,
               content_hash: str = None,
               content_hash_full: str = None,
               created_at: str = None) -> tuple["DocumentRecord", bool]:
        if collection not in self._data:
            self._data[collection] = {}
        existed = id in self._data[collection]
        content_changed = (
            existed
            and content_hash is not None
            and self._data[collection][id].get("content_hash") != content_hash
        )
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        existing_created = self._data[collection].get(id, {}).get("created_at")
        created_at = existing_created or created_at or now
        self._data[collection][id] = {
            "summary": summary,
            "tags": tags,
            "content_hash": content_hash,
            "content_hash_full": content_hash_full,
            "created_at": created_at,
            "updated_at": now,
        }
        return (self._make_record(collection, id, self._data[collection][id]), content_changed)

    def get(self, collection: str, id: str):
        if collection not in self._data or id not in self._data[collection]:
            return None
        return self._make_record(collection, id, self._data[collection][id])

    def get_many(self, collection: str, ids: list[str]) -> dict:
        result = {}
        for id in ids:
            rec = self.get(collection, id)
            if rec is not None:
                result[id] = rec
        return result

    def exists(self, collection: str, id: str) -> bool:
        return collection in self._data and id in self._data[collection]

    def insert_if_absent(self, collection: str, id: str, summary: str,
                         tags: dict, created_at: str = None) -> bool:
        if self.exists(collection, id):
            return False
        self.upsert(collection, id, summary, tags, created_at=created_at)
        return True

    def find_by_content_hash(self, collection: str, content_hash: str, *,
                             content_hash_full: str = "", exclude_id: str = ""):
        if collection not in self._data or not content_hash:
            return None
        for doc_id, rec in self._data[collection].items():
            if doc_id != exclude_id and rec.get("content_hash") == content_hash:
                # Verify full hash if both sides have one
                if content_hash_full and rec.get("content_hash_full"):
                    if content_hash_full != rec["content_hash_full"]:
                        continue
                return self._make_record(collection, doc_id, rec)
        return None

    def delete(self, collection: str, id: str, delete_versions: bool = True) -> bool:
        if collection in self._data and id in self._data[collection]:
            del self._data[collection][id]
            return True
        return False

    def list_ids(self, collection: str, limit: int = None) -> list[str]:
        ids = list(self._data.get(collection, {}).keys())
        return ids[:limit] if limit else ids

    def count(self, collection: str = None) -> int:
        if collection:
            return len(self._data.get(collection, {}))
        return sum(len(c) for c in self._data.values())

    def version_count(self, collection: str, id: str) -> int:
        return 0

    def update_tags(self, collection: str, id: str, tags: dict) -> bool:
        if collection in self._data and id in self._data[collection]:
            self._data[collection][id]["tags"] = tags
            return True
        return False

    def update_summary(self, collection: str, id: str, summary: str) -> bool:
        if collection in self._data and id in self._data[collection]:
            self._data[collection][id]["summary"] = summary
            return True
        return False

    def update_content_hash(self, collection: str, id: str,
                            content_hash: str, content_hash_full: str) -> bool:
        if collection in self._data and id in self._data[collection]:
            self._data[collection][id]["content_hash"] = content_hash
            self._data[collection][id]["content_hash_full"] = content_hash_full
            return True
        return False

    def list_recent(
        self,
        collection: str,
        limit: int = 10,
        order_by: str = "updated",
        offset: int = 0,
    ) -> list:
        if collection not in self._data:
            return []
        records = []
        rows = list(self._data[collection].items())
        for id, rec in rows[offset:offset + limit]:
            records.append(self._make_record(collection, id, rec))
        return records

    def query_by_tag_key(self, collection: str, key: str, limit: int = 100,
                         since_date: str = None, until_date: str = None, offset: int = 0) -> list:
        if collection not in self._data:
            return []
        results = []
        for id, rec in self._data[collection].items():
            if key not in rec["tags"]:
                continue
            updated = rec.get("updated_at", "")
            if since_date and updated < since_date:
                continue
            if until_date and updated >= until_date:
                continue
            results.append(self._make_record(collection, id, rec))
        return results[offset:offset + limit]

    def list_distinct_tag_keys(self, collection: str) -> list[str]:
        keys = set()
        for rec in self._data.get(collection, {}).values():
            for k in rec["tags"]:
                if not k.startswith("_"):
                    keys.add(k)
        return sorted(keys)

    def list_distinct_tag_values(self, collection: str, key: str) -> list[str]:
        values = set()
        for rec in self._data.get(collection, {}).values():
            if key in rec["tags"]:
                for value in tag_values(rec["tags"], key):
                    values.add(value)
        return sorted(values)

    def tag_pair_counts(self, collection: str) -> dict[tuple[str, str], int]:
        counts: dict[tuple[str, str], int] = {}
        for rec in self._data.get(collection, {}).values():
            for k in rec["tags"]:
                if not k.startswith("_"):
                    for value in set(tag_values(rec["tags"], k)):
                        counts[(k, value)] = counts.get((k, value), 0) + 1
        return counts

    def max_version(self, collection: str, id: str) -> int:
        return 0

    def copy_record(self, collection: str, from_id: str, to_id: str):
        if collection not in self._data or from_id not in self._data[collection]:
            return None
        if to_id in self._data[collection]:
            return None
        self._data[collection][to_id] = dict(self._data[collection][from_id])
        return self.get(collection, to_id)

    def get_version(self, collection: str, id: str, offset: int = 0):
        return None

    def list_versions(self, collection: str, id: str, limit: int = 10) -> list:
        return []

    def list_versions_around(self, collection: str, id: str,
                             version: int, radius: int = 2) -> list:
        return []

    def get_version_nav(self, collection: str, id: str,
                        current_version=None, limit: int = 3) -> dict:
        return {"prev": []}

    def restore_latest_version(self, collection: str, id: str):
        return None

    def list_collections(self) -> list[str]:
        return list(self._data.keys())

    def touch(self, collection: str, id: str) -> None:
        pass

    def query_fts(self, collection: str, query: str, limit: int = 10,
                  tags: dict = None) -> list[tuple[str, str, float]]:
        """Mock FTS5 search — case-insensitive OR-token matching on summaries + parts."""
        def _matches_filter(item_tags: dict, filt: dict | None) -> bool:
            if not filt:
                return True
            for key in filt:
                wanted = set(tag_values(filt, key))
                if wanted and not wanted.issubset(set(tag_values(item_tags, key))):
                    return False
            return True

        tokens = [t.lower() for t in query.split()]
        if not tokens:
            return []
        results = []
        # Search document summaries
        for id, rec in self._data.get(collection, {}).items():
            summary_lower = rec["summary"].lower()
            if not any(t in summary_lower for t in tokens):
                continue
            rec_tags = rec.get("tags", {})
            if not _matches_filter(rec_tags, tags):
                continue
            results.append((id, rec["summary"], -1.0))
        # Search parts (summary + content)
        for key, parts in self._parts.items():
            # key format: _parts:{collection}:{id}
            parts_coll = key.split(":", 2)[1] if ":" in key else ""
            parts_id = key.split(":", 2)[2] if key.count(":") >= 2 else ""
            if parts_coll != collection:
                continue
            for part in parts:
                text = (part.summary + " " + part.content).lower()
                if any(t in text for t in tokens):
                    if not _matches_filter(part.tags, tags):
                        continue
                    results.append((f"{parts_id}@p{part.part_num}", part.summary, -1.0))
        return results[:limit]

    def query_fts_scoped(self, collection: str, query: str, ids: list[str],
                         limit: int = 10,
                         tags: dict = None) -> list[tuple[str, str, float]]:
        """Mock scoped FTS — delegates to query_fts then filters to ids."""
        all_results = self.query_fts(collection, query, limit=limit * 3, tags=tags)
        id_set = set(ids)
        filtered = []
        for r in all_results:
            base = r[0].split("@")[0] if "@" in r[0] else r[0]
            if base in id_set:
                filtered.append(r)
        return filtered[:limit]

    def get_stopwords(self) -> frozenset[str]:
        try:
            from importlib.resources import files
            stop_text = files("keep.data.system").joinpath("stop.md").read_text()
        except Exception:
            return frozenset()
        words = set()
        in_frontmatter = False
        for line in stop_text.splitlines():
            stripped = line.strip()
            if stripped == "---":
                in_frontmatter = not in_frontmatter
                continue
            if in_frontmatter or not stripped or stripped.startswith("#"):
                continue
            words.add(stripped.lower())
        return frozenset(words)

    @property
    def _fts_available(self) -> bool:
        return True

    def query_by_id_prefix(
        self,
        collection: str,
        prefix: str,
        limit: int | None = None,
        offset: int = 0,
    ) -> list:
        if collection not in self._data:
            return []
        results = []
        for id, rec in self._data[collection].items():
            if id.startswith(prefix):
                results.append(self._make_record(collection, id, rec))
        if limit is None:
            return results[offset:]
        return results[offset:offset + limit]

    def touch_many(self, collection: str, ids: list[str]) -> None:
        pass

    # -- Part methods --

    def upsert_parts(self, collection: str, id: str, parts: list) -> int:
        key = f"_parts:{collection}:{id}"
        self._parts[key] = parts
        return len(parts)

    def upsert_single_part(self, collection: str, id: str, part) -> None:
        key = f"_parts:{collection}:{id}"
        parts = self._parts.setdefault(key, [])
        # Replace existing part with same part_num, or append
        for i, p in enumerate(parts):
            if p.part_num == part.part_num:
                parts[i] = part
                return
        parts.append(part)

    def get_part(self, collection: str, id: str, part_num: int):
        key = f"_parts:{collection}:{id}"
        parts = self._parts.get(key, [])
        for p in parts:
            if p.part_num == part_num:
                return p
        return None

    def list_parts(self, collection: str, id: str) -> list:
        key = f"_parts:{collection}:{id}"
        return sorted(self._parts.get(key, []), key=lambda p: p.part_num)

    def part_count(self, collection: str, id: str) -> int:
        key = f"_parts:{collection}:{id}"
        return len(self._parts.get(key, []))

    def delete_parts(self, collection: str, id: str) -> int:
        key = f"_parts:{collection}:{id}"
        parts = self._parts.pop(key, [])
        return len(parts)

    def update_part_tags(self, collection: str, id: str, part_num: int, tags: dict) -> bool:
        key = f"_parts:{collection}:{id}"
        parts = self._parts.get(key, [])
        for i, p in enumerate(parts):
            if p.part_num == part_num:
                from keep.document_store import PartInfo
                parts[i] = PartInfo(
                    part_num=p.part_num,
                    summary=p.summary,
                    tags=tags,
                    content=p.content,
                    created_at=p.created_at,
                )
                return True
        return False

    # -- Edge methods --

    def __init_edges(self):
        if not hasattr(self, "_edges"):
            self._edges: list[dict] = []
            self._backfills: dict[tuple[str, str], dict] = {}

    def upsert_edge(self, collection: str, source_id: str, predicate: str,
                    target_id: str, inverse: str, created: str) -> None:
        self.__init_edges()
        # Replace existing edge with same PK
        self._edges = [
            e for e in self._edges
            if not (e["source_id"] == source_id and e["collection"] == collection
                    and e["predicate"] == predicate and e["target_id"] == target_id)
        ]
        self._edges.append({
            "source_id": source_id, "collection": collection,
            "predicate": predicate, "target_id": target_id,
            "inverse": inverse, "created": created,
        })

    def upsert_edges_batch(self, collection: str,
                           edges: list[tuple[str, str, str, str, str]]) -> int:
        for source_id, predicate, target_id, inverse, created in edges:
            self.upsert_edge(collection, source_id, predicate, target_id, inverse, created)
        return len(edges)

    def delete_edges_batch(self, collection: str,
                           edges: list[tuple[str, str, str]]) -> int:
        count = 0
        for source_id, predicate, target_id in edges:
            count += self.delete_edge(collection, source_id, predicate, target_id)
        return count

    def delete_edge(
        self, collection: str, source_id: str, predicate: str, target_id: str | None = None
    ) -> int:
        self.__init_edges()
        before = len(self._edges)
        self._edges = [
            e for e in self._edges
            if not (e["source_id"] == source_id and e["collection"] == collection
                    and e["predicate"] == predicate
                    and (target_id is None or e["target_id"] == target_id))
        ]
        return before - len(self._edges)

    def delete_edges_for_source(self, collection: str, source_id: str) -> int:
        self.__init_edges()
        before = len(self._edges)
        self._edges = [
            e for e in self._edges
            if not (e["collection"] == collection and e["source_id"] == source_id)
        ]
        return before - len(self._edges)

    def delete_edges_for_target(self, collection: str, target_id: str) -> int:
        self.__init_edges()
        before = len(self._edges)
        self._edges = [
            e for e in self._edges
            if not (e["collection"] == collection and e["target_id"] == target_id)
        ]
        return before - len(self._edges)

    def delete_edges_for_predicate(self, collection: str, predicate: str) -> int:
        self.__init_edges()
        before = len(self._edges)
        self._edges = [
            e for e in self._edges
            if not (e["collection"] == collection and e["predicate"] == predicate)
        ]
        return before - len(self._edges)

    def delete_version_edges_for_source(self, collection: str, source_id: str) -> int:
        return 0

    def delete_version_edges_for_target(self, collection: str, target_id: str) -> int:
        return 0

    def delete_version_edges_for_predicate(self, collection: str, predicate: str) -> int:
        return 0

    def backfill_version_edges_for_predicate(
        self, collection: str, predicate: str, inverse: str
    ) -> int:
        return 0

    def get_inverse_edges(self, collection: str, target_id: str) -> list[tuple[str, str, str]]:
        self.__init_edges()
        results = [
            (e["inverse"], e["source_id"], e["created"])
            for e in self._edges
            if e["collection"] == collection and e["target_id"] == target_id
        ]
        # Match real store: ORDER BY inverse ASC, created DESC
        results.sort(key=lambda r: (r[0], r[2]), reverse=False)
        # Group by inverse, reverse created within each group
        from itertools import groupby
        ordered = []
        for _, group in groupby(results, key=lambda r: r[0]):
            items = list(group)
            items.sort(key=lambda r: r[2], reverse=True)
            ordered.extend(items)
        return ordered

    def get_forward_edges(self, collection: str, source_id: str) -> list[tuple[str, str, str]]:
        self.__init_edges()
        results = [
            (e["predicate"], e["target_id"], e["created"])
            for e in self._edges
            if e["collection"] == collection and e["source_id"] == source_id
        ]
        results.sort(key=lambda r: (r[0], r[2]), reverse=False)
        from itertools import groupby
        ordered = []
        for _, group in groupby(results, key=lambda r: r[0]):
            items = list(group)
            items.sort(key=lambda r: r[2], reverse=True)
            ordered.extend(items)
        return ordered

    def find_edge_targets(self, collection: str, query: str) -> list[str]:
        import re
        self.__init_edges()
        if not query:
            return []
        query_lower = query.lower()
        targets = set()
        for e in self._edges:
            if e["collection"] != collection:
                continue
            tid = e["target_id"]
            escaped = re.escape(tid.lower())
            left = r'\b' if re.match(r'\w', tid) else r'(?:^|(?<=\s))'
            right = r'\b' if re.search(r'\w$', tid) else r'(?=\s|$)'
            pattern = left + escaped + right
            if re.search(pattern, query_lower):
                targets.add(tid)
        return list(targets)

    def has_edges(self, collection: str) -> bool:
        self.__init_edges()
        return any(e["collection"] == collection for e in self._edges)

    def backfill_exists(self, collection: str, predicate: str) -> bool:
        self.__init_edges()
        return (collection, predicate) in self._backfills

    def get_backfill_status(self, collection: str, predicate: str) -> str | None:
        self.__init_edges()
        rec = self._backfills.get((collection, predicate))
        if rec is None:
            return None
        return rec.get("completed")

    def upsert_backfill(self, collection: str, predicate: str, inverse: str,
                        completed: str | None = None) -> None:
        self.__init_edges()
        self._backfills[(collection, predicate)] = {
            "inverse": inverse, "completed": completed,
        }

    def delete_backfill(self, collection: str, predicate: str) -> None:
        self.__init_edges()
        self._backfills.pop((collection, predicate), None)

    def close(self) -> None:
        self._data.clear()


class MockPendingSummaryQueue:
    """Mock pending summary queue."""

    def __init__(self, db_path: Path):
        self._queue = []

    def enqueue(self, id: str, collection: str, content: str, **kwargs) -> None:
        """Add item to pending queue."""
        self._queue.append({"id": id, "collection": collection, "content": content, **kwargs})

    def dequeue(self, limit: int = 10) -> list:
        """Get items from queue."""
        return []

    def complete(self, id: str, collection: str) -> None:
        """Mark item as complete."""
        pass

    def fail(self, id: str, collection: str) -> None:
        """Mark item as failed."""
        pass

    def count(self) -> int:
        """Count pending items."""
        return len(self._queue)

    def mark_delegated(self, id: str, collection: str, task_type: str, remote_task_id: str) -> None:
        pass

    def list_delegated(self) -> list:
        return []

    def count_delegated(self) -> int:
        return 0

    def close(self) -> None:
        self._queue.clear()


@pytest.fixture
def mock_providers():
    """
    Fixture that patches all providers AND stores to use mocks.

    This avoids loading:
    - Real ML models (sentence-transformers, etc.)
    - Real ChromaDB
    - Real SQLite document store

    Use this fixture for tests that create Keeper instances but don't
    need real ML model or database behavior.

    Usage:
        def test_something(mock_providers, tmp_path):
            kp = Keeper(store_path=tmp_path)
            # ... test using mocked providers and stores
    """
    mock_embed = MockEmbeddingProvider()
    mock_summ = MockSummarizationProvider()
    mock_doc = MockDocumentProvider()

    mock_reg = MagicMock()
    mock_reg.create_document.return_value = mock_doc
    mock_reg.create_embedding.return_value = mock_embed
    mock_reg.create_summarization.return_value = mock_summ

    with patch("keep.api.get_registry", return_value=mock_reg), \
         patch("keep.api.CachingEmbeddingProvider", side_effect=lambda p, **kw: p), \
         patch("keep.store.ChromaStore", MockChromaStore), \
         patch("keep.document_store.DocumentStore", MockDocumentStore), \
         patch("keep.pending_summaries.PendingSummaryQueue", MockPendingSummaryQueue), \
         patch("keep.api.Keeper._spawn_processor", return_value=False):
        yield {
            "embedding": mock_embed,
            "summarization": mock_summ,
            "document": mock_doc,
            "registry": mock_reg,
        }


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (loading real ML models)"
    )
    config.addinivalue_line(
        "markers", "e2e: marks tests as end-to-end (require real providers)"
    )
