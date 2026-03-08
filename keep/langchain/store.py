"""KeepStore — LangGraph BaseStore backed by Keep reflective memory.

Maps LangGraph's namespace/key model to Keep's document model using
configurable positional tag mapping:

- Namespace components become regular Keep tags via ``namespace_keys``
- Document IDs are namespace/key paths joined by "/"
- ``value[content_key]`` becomes Keep text; other string values become tags
- Search uses Keep's vector similarity with tag pre-filtering

Usage::

    from keep.langchain import KeepStore

    store = KeepStore()                    # default store
    store = KeepStore(store="~/.keep")     # explicit path
    store = KeepStore(keeper=my_keeper)    # existing Keeper instance

    # Custom namespace mapping
    store = KeepStore(namespace_keys=["category", "user"])

    # With LangGraph
    from langgraph.graph import StateGraph
    graph = StateGraph(...)
    graph.compile(store=store)

    # langmem tools work automatically
    from langmem import create_manage_memory_tool, create_search_memory_tool
    tools = [
        create_manage_memory_tool(namespace=("memories", "{user_id}")),
        create_search_memory_tool(namespace=("memories", "{user_id}")),
    ]
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterable

try:
    from langgraph.store.base import (
        BaseStore,
        GetOp,
        Item as LGItem,
        ListNamespacesOp,
        MatchCondition,
        PutOp,
        SearchItem,
        SearchOp,
    )
except ImportError as e:
    raise ImportError(
        "langgraph is required for KeepStore. "
        "Install with: pip install langgraph"
    ) from e

from keep.api import Keeper
from keep.types import Item as KeepItem, parse_utc_timestamp

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────

_SOURCE_TAG = "_source"        # Keep convention: provenance marker
_SOURCE_VALUE = "langchain"    # Identifies KeepStore-managed items
_DATA_TAG = "_keep_data"       # JSON for non-string value entries

# Default namespace key mapping: single-level user scoping
_DEFAULT_NS_KEYS: list[str] = ["user"]
_DEFAULT_CONTENT_KEY = "content"

# Result type from batch() — matches langgraph's type alias
_Result = LGItem | list[LGItem] | list[SearchItem] | list[tuple[str, ...]] | None
# Op type — union of all operation types
_Op = GetOp | SearchOp | PutOp | ListNamespacesOp


# ── Helpers ───────────────────────────────────────────────────────────

def _namespace_to_id(namespace: tuple[str, ...], key: str) -> str:
    """Join namespace tuple and key into a Keep document ID."""
    return "/".join((*namespace, key))


def _id_to_namespace_key(
    doc_id: str,
    depth: int,
) -> tuple[tuple[str, ...], str]:
    """Split a Keep document ID into (namespace_tuple, key).

    Uses the known depth to split precisely, allowing keys that
    contain "/" characters.
    """
    parts = doc_id.split("/")
    if depth > 0 and depth < len(parts):
        return tuple(parts[:depth]), "/".join(parts[depth:])
    if depth == 0:
        return (), doc_id
    # Fallback: last segment is key
    if len(parts) > 1:
        return tuple(parts[:-1]), parts[-1]
    return (), parts[0]


def _namespace_to_tags(
    namespace: tuple[str, ...],
    namespace_keys: list[str],
) -> dict[str, str]:
    """Map a namespace tuple to Keep tags using positional keys.

    Example::

        _namespace_to_tags(("memories", "alice"), ["category", "user"])
        # → {"category": "memories", "user": "alice"}
    """
    tags: dict[str, str] = {}
    for i, part in enumerate(namespace):
        if i < len(namespace_keys):
            tags[namespace_keys[i]] = part
    return tags


def _tags_to_namespace(
    tags: dict[str, str],
    namespace_keys: list[str],
) -> tuple[str, ...]:
    """Reconstruct a namespace tuple from tags using positional keys.

    Stops at the first missing key to determine depth.

    Example::

        _tags_to_namespace({"category": "memories", "user": "alice"}, ["category", "user"])
        # → ("memories", "alice")
    """
    parts: list[str] = []
    for key in namespace_keys:
        val = tags.get(key)
        if val is None or val == "":
            break
        parts.append(val)
    return tuple(parts)


def _infer_depth(
    tags: dict[str, str],
    namespace_keys: list[str],
) -> int:
    """Infer namespace depth from which consecutive keys are present."""
    depth = 0
    for key in namespace_keys:
        if key in tags and tags[key]:
            depth += 1
        else:
            break
    return depth


def _extract_text(
    value: dict[str, Any],
    index: list[str] | None,
) -> str:
    """Extract text from a value dict for embedding.

    Args:
        value: The value dict to extract text from.
        index: If a list, only these fields are used. If None, all string values.
    """
    if isinstance(index, list):
        parts = [str(value[k]) for k in index if k in value]
    else:
        parts = [str(v) for v in value.values() if isinstance(v, str)]
    return " ".join(parts) if parts else json.dumps(value)


def _parse_ts(ts_str: str | None) -> datetime:
    """Parse a Keep timestamp string, falling back to now."""
    if not ts_str:
        return datetime.now(timezone.utc)
    try:
        return parse_utc_timestamp(ts_str)
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


def _matches_condition(
    ns: tuple[str, ...],
    cond: MatchCondition,
) -> bool:
    """Check if a namespace matches a prefix/suffix condition."""
    path = cond.path
    if len(path) > len(ns):
        return False
    if cond.match_type == "prefix":
        return all(p == "*" or p == n for p, n in zip(path, ns))
    elif cond.match_type == "suffix":
        return all(
            p == "*" or p == n
            for p, n in zip(reversed(path), reversed(ns))
        )
    return True


# ── KeepStore ─────────────────────────────────────────────────────────

class KeepStore(BaseStore):
    """LangGraph BaseStore backed by Keep reflective memory.

    Args:
        keeper: An existing Keeper instance to wrap. If None, a new one
            is created from ``store`` path.
        store: Path to the Keep store directory (e.g. ``"~/.keep"``).
            Ignored if ``keeper`` is provided.
        user_id: Optional user scope. When set, all operations auto-add
            a ``user`` tag for multi-user isolation.
        namespace_keys: Positional mapping from namespace components to
            Keep tag names. Default: ``["user"]``.
        content_key: Which key in the value dict holds the text content
            that becomes Keep's document text. Default: ``"content"``.
    """

    def __init__(
        self,
        keeper: Keeper | None = None,
        store: str | None = None,
        user_id: str | None = None,
        namespace_keys: list[str] | None = None,
        content_key: str = _DEFAULT_CONTENT_KEY,
    ):
        super().__init__()
        if keeper is not None:
            self._keeper = keeper
        else:
            self._keeper = Keeper(store_path=store)
            self._keeper._get_embedding_provider()
        self._user_id = user_id
        # Resolve namespace_keys: explicit > config > default
        if namespace_keys is not None:
            self._namespace_keys = namespace_keys
        elif self._keeper._config.namespace_keys:
            self._namespace_keys = list(self._keeper._config.namespace_keys)
        else:
            self._namespace_keys = list(_DEFAULT_NS_KEYS)
        self._content_key = content_key
        # Set for fast lookup when excluding namespace tags from value recovery
        self._ns_keys_set = set(self._namespace_keys)

    @property
    def keeper(self) -> Keeper:
        """The underlying Keeper instance."""
        return self._keeper

    def batch(self, ops: Iterable[_Op]) -> list[_Result]:
        results: list[_Result] = []
        for op in ops:
            if isinstance(op, GetOp):
                results.append(self._handle_get(op))
            elif isinstance(op, SearchOp):
                results.append(self._handle_search(op))
            elif isinstance(op, PutOp):
                self._handle_put(op)
                results.append(None)
            elif isinstance(op, ListNamespacesOp):
                results.append(self._handle_list_namespaces(op))
            else:
                raise ValueError(f"Unknown op type: {type(op)}")
        return results

    async def abatch(self, ops: Iterable[_Op]) -> list[_Result]:
        # Keeper is synchronous; async wrapper for the interface.
        return self.batch(list(ops))

    # ── Op handlers ──────────────────────────────────────────────────

    def _handle_get(self, op: GetOp) -> LGItem | None:
        doc_id = _namespace_to_id(op.namespace, op.key)
        item = self._keeper.get(doc_id)
        if item is None:
            return None
        return self._to_lg_item(item, op.namespace, op.key)

    def _handle_search(self, op: SearchOp) -> list[SearchItem]:
        # Tag filter from namespace prefix
        filter_tags = (
            _namespace_to_tags(op.namespace_prefix, self._namespace_keys)
            if op.namespace_prefix
            else {}
        )

        # Merge LangGraph filter (simple key=value equality only)
        if op.filter:
            for k, v in op.filter.items():
                if isinstance(v, dict):
                    # Operator filters ($gt, $lt, etc.) — not supported
                    logger.warning(
                        "Operator filters not supported, ignoring: %s", k
                    )
                    continue
                filter_tags[k] = str(v)

        # Add user_id scoping
        if self._user_id and "user" not in filter_tags:
            filter_tags["user"] = self._user_id

        tags = filter_tags or None

        if op.query:
            items = self._keeper.find(op.query, tags=tags, limit=op.limit)
        else:
            items = self._keeper.list_items(tags=tags, limit=op.limit)

        # Convert to SearchItems
        results: list[SearchItem] = []
        for item in items:
            depth = _infer_depth(item.tags, self._namespace_keys)
            ns, key = _id_to_namespace_key(item.id, depth)
            results.append(self._to_search_item(item, ns, key))
        return results

    def _handle_put(self, op: PutOp) -> None:
        doc_id = _namespace_to_id(op.namespace, op.key)

        if op.value is None:
            # Delete — PutOp with value=None
            self._keeper.delete(doc_id)
            return

        # --- Tags from namespace mapping ---
        user_tags = _namespace_to_tags(op.namespace, self._namespace_keys)

        # --- Extract content and extra tags from value dict ---
        content = ""
        non_string_data: dict[str, Any] = {}

        for k, v in op.value.items():
            if k == self._content_key:
                content = str(v) if v is not None else ""
            elif isinstance(v, str):
                user_tags[k] = v
            else:
                non_string_data[k] = v

        # user_id scoping
        if self._user_id and "user" not in user_tags:
            user_tags["user"] = self._user_id

        # Enforce required_tags (mirrors Keeper.put() check).
        # System docs (dot-prefix IDs) are exempt.
        required = self._keeper._config.required_tags
        if required and not doc_id.startswith("."):
            non_system = {k: v for k, v in user_tags.items()
                         if not k.startswith("_")}
            missing = [t for t in required if t not in non_system]
            if missing:
                raise ValueError(f"Required tags missing: {', '.join(missing)}")

        # System tags (minimal: source marker + non-string overflow)
        system_tags: dict[str, str] = {_SOURCE_TAG: _SOURCE_VALUE}
        if non_string_data:
            system_tags[_DATA_TAG] = json.dumps(
                non_string_data, separators=(",", ":")
            )

        # Determine content text for embedding
        index = op.index if isinstance(op.index, list) else None
        if op.index is False:
            # Not indexed — store content as summary (no embedding generated)
            summary_text = content or _extract_text(op.value, None) or op.key
            self._keeper._upsert(
                doc_id,
                op.key,
                tags=user_tags,
                summary=summary_text,
                system_tags=system_tags,
            )
        else:
            embed_text = content
            if isinstance(op.index, list):
                embed_text = _extract_text(op.value, op.index)
            elif not embed_text:
                embed_text = _extract_text(op.value, None)

            self._keeper._upsert(
                doc_id,
                embed_text,
                tags=user_tags,
                system_tags=system_tags,
            )

    def _handle_list_namespaces(
        self,
        op: ListNamespacesOp,
    ) -> list[tuple[str, ...]]:
        # Find all KeepStore-managed items via _source=langchain tag.
        # Note: this is a full scan, capped at 10K items. Acceptable for
        # stores with <10K KeepStore items. For scale, a _namespaces index
        # table maintained on write would replace this scan.
        items = self._keeper.list_items(
            tags={_SOURCE_TAG: _SOURCE_VALUE}, limit=10000,
        )

        # Extract unique namespaces from tag values
        seen: set[tuple[str, ...]] = set()
        for item in items:
            ns = _tags_to_namespace(item.tags, self._namespace_keys)

            # Apply max_depth truncation
            if op.max_depth is not None and len(ns) > op.max_depth:
                ns = ns[: op.max_depth]

            seen.add(ns)

        # Apply match conditions
        result = list(seen)
        if op.match_conditions:
            for cond in op.match_conditions:
                result = [ns for ns in result if _matches_condition(ns, cond)]

        # Sort for deterministic output, then apply offset/limit
        result.sort()
        return result[op.offset : op.offset + op.limit]

    # ── Value recovery ───────────────────────────────────────────────

    def _recover_value(self, item: KeepItem) -> dict[str, Any]:
        """Reconstruct a LangGraph value dict from a Keep item.

        Inverse of the put mapping:
        - summary → {content_key: summary}
        - Non-system, non-namespace tags → value entries
        - _keep_data → merged in (non-string values)
        """
        value: dict[str, Any] = {self._content_key: item.summary}

        # Add string tags (exclude namespace keys and system tags)
        for k, v in item.tags.items():
            if k.startswith("_"):
                continue
            if k in self._ns_keys_set:
                continue
            value[k] = v

        # Merge non-string data from overflow tag
        data_json = item.tags.get(_DATA_TAG)
        if data_json:
            try:
                value.update(json.loads(data_json))
            except (json.JSONDecodeError, TypeError):
                pass

        return value

    def _to_lg_item(
        self,
        item: KeepItem,
        namespace: tuple[str, ...],
        key: str,
    ) -> LGItem:
        """Convert a Keep Item to a LangGraph Item."""
        return LGItem(
            namespace=namespace,
            key=key,
            value=self._recover_value(item),
            created_at=_parse_ts(item.tags.get("_created")),
            updated_at=_parse_ts(item.tags.get("_updated")),
        )

    def _to_search_item(
        self,
        item: KeepItem,
        namespace: tuple[str, ...],
        key: str,
    ) -> SearchItem:
        """Convert a Keep Item to a LangGraph SearchItem."""
        return SearchItem(
            namespace=namespace,
            key=key,
            value=self._recover_value(item),
            created_at=_parse_ts(item.tags.get("_created")),
            updated_at=_parse_ts(item.tags.get("_updated")),
            score=item.score,
        )
