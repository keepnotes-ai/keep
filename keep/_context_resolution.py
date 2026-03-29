"""Context resolution mixin for Keeper.

Extracts display-context assembly, prompt rendering, meta-doc resolution,
similar-for-display, and ranking logic from the main Keeper class.
"""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Any, Optional

from .types import (
    Item,
    ItemContext,
    SimilarRef,
    MetaRef,
    EdgeRef,
    VersionRef,
    PartRef,
    PromptResult,
    PromptInfo,
    TagMap,
    is_system_id,
    local_date,
    normalize_id,
    parse_version_ref,
    is_part_id,
)
from .utils import _is_hidden, _parse_meta_doc

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class ContextResolutionMixin:
    """Mixin providing context assembly, prompt rendering, and meta resolution.

    Expects the host class to provide:
    - ``_resolve_chroma_collection() -> str``
    - ``_resolve_doc_collection() -> str``
    - ``_document_store``
    - ``_store``
    - ``_needs_sysdoc_migration: bool``
    - ``_migrate_system_documents()``
    - ``_run_read_flow(name, params, **kw)``
    - ``_resolve_edge_refs(item, id) -> dict``
    - ``_apply_recency_decay(items) -> list``  (from SearchAugmentationMixin)
    - ``get(id) -> Item | None``
    - ``get_version(id, offset) -> Item | None``
    - ``get_version_nav(id, version) -> dict``
    - ``get_now() -> Item``
    - ``find(...) -> list``
    - ``list_items(...) -> list``
    """

    # ------------------------------------------------------------------
    # Flow output mappers (static)
    # ------------------------------------------------------------------

    @staticmethod
    def _map_flow_similar(binding: dict) -> list:
        """Map find action output to SimilarRef list."""
        refs = []
        for r in binding.get("results", []):
            if not isinstance(r, dict):
                continue
            tags = r.get("tags") or {}
            refs.append(SimilarRef(
                id=tags.get("_base_id", r.get("id", "")),
                offset=0,
                score=r.get("score"),
                date=local_date(tags.get("_updated") or tags.get("_created", "")),
                summary=r.get("summary", ""),
            ))
        return refs

    @staticmethod
    def _map_flow_meta(binding: dict) -> dict:
        """Map resolve_meta action output to MetaRef dict."""
        refs: dict[str, list] = {}
        for name, items in (binding.get("sections") or {}).items():
            if not isinstance(items, list):
                continue
            refs[name] = [
                MetaRef(id=r.get("id", ""), summary=r.get("summary", ""))
                for r in items if isinstance(r, dict)
            ]
        return refs

    @staticmethod
    def _map_flow_parts(binding: dict) -> list:
        """Map find action (prefix mode) output to PartRef list."""
        refs = []
        for r in binding.get("results", []):
            if not isinstance(r, dict):
                continue
            rid = r.get("id", "")
            # Extract part_num from ID like "base@p3"
            part_num = 0
            if "@p" in rid:
                suffix = rid.rsplit("@p", 1)[-1]
                try:
                    part_num = int(suffix)
                except ValueError:
                    pass
            refs.append(PartRef(
                part_num=part_num,
                summary=r.get("summary", ""),
                tags=r.get("tags") or {},
            ))
        return refs

    @staticmethod
    def _map_flow_edges(binding: dict) -> dict:
        """Map resolve_edges action output to {predicate: [EdgeRef]} dict."""
        result: dict[str, list] = {}
        edges = binding.get("edges", {})
        if not isinstance(edges, dict):
            return result
        for pred, entries in edges.items():
            if not isinstance(entries, list):
                continue
            refs = []
            for e in entries:
                if not isinstance(e, dict):
                    continue
                refs.append(EdgeRef(
                    source_id=e.get("id", ""),
                    date=e.get("date", ""),
                    summary=e.get("summary", ""),
                ))
            if refs:
                result[pred] = refs
        return result

    # ------------------------------------------------------------------
    # Display context assembly
    # ------------------------------------------------------------------

    def get_context(
        self,
        id: str,
        *,
        version: int | None = None,
        similar_limit: int = 3,
        meta_limit: int = 3,
        parts_limit: int = 10,
        edges_limit: int = 5,
        versions_limit: int = 3,
        include_similar: bool = True,
        include_meta: bool = True,
        include_parts: bool = True,
        include_versions: bool = True,
    ) -> ItemContext | None:
        """Assemble complete display context for a single item.

        Single implementation of the 5-call assembly pattern used by
        CLI get, now, and put commands.  Returns None if the item
        doesn't exist.

        Args:
            id: Document identifier
            version: Public version selector:
                - None or 0: current
                - N > 0: offset from current (1=previous)
                - N < 0: archived ordinal from oldest (-1=oldest)
            similar_limit: Max similar items to include
            meta_limit: Max items per meta-doc section
            parts_limit: Max parts to include
            versions_limit: Max prev/next versions to include
            include_similar: Whether to resolve similar items
            include_meta: Whether to resolve meta-doc sections
            edges_limit: Max edges per direction to include
            include_parts: Whether to include parts manifest
            include_versions: Whether to include version navigation
        """
        from .perf_stats import perf
        import time
        _ctx_t0 = time.monotonic()

        # Ensure state docs are in the store for the read flow.
        self._ensure_sysdocs()

        # Parse @V{N} version ref from ID (explicit version= takes precedence)
        base_id, id_version = parse_version_ref(id)
        if id_version is not None and version is None:
            version = id_version
            id = base_id
        id = normalize_id(id)
        resolved = self.resolve_version_offset(id, version)
        if resolved is None:
            return None
        offset = resolved
        if offset > 0:
            item = self.get_version(id, offset)
        else:
            item = self.get(id)
        if item is None:
            return None

        # System docs are authored reference/configuration content.
        # Rendering them should show the document itself only; surrounding
        # context assembly (similar/meta/parts/edges/version nav) adds noise
        # and unnecessary work.
        if is_system_id(item.id):
            perf.record("get_context", "total", time.monotonic() - _ctx_t0,
                        context_id=id)
            return ItemContext(
                item=item,
                viewing_offset=offset,
                similar=[],
                meta={},
                edges={},
                parts=[],
                prev=[],
                next=[],
            )

        # Version navigation
        prev_refs: list[VersionRef] = []
        next_refs: list[VersionRef] = []
        if include_versions:
            if offset == 0:
                nav = self.get_version_nav(id, None, limit=versions_limit)
                for i, v in enumerate(nav.get("prev", [])[:versions_limit]):
                    prev_refs.append(VersionRef(
                        offset=i + 1,
                        date=local_date(v.tags.get("_created") or v.created_at or ""),
                        summary=v.summary,
                    ))
            else:
                # Keep navigation in user-visible offset space.
                # This avoids mixing offset (V{N}) with internal version numbers.
                older = self.get_version(id, offset + 1)
                if older is not None:
                    prev_refs.append(VersionRef(
                        offset=offset + 1,
                        date=local_date(older.tags.get("_created", "")),
                        summary=older.summary,
                    ))
                if offset > 1:
                    newer = self.get_version(id, offset - 1)
                    if newer is not None:
                        next_refs.append(VersionRef(
                            offset=offset - 1,
                            date=local_date(newer.tags.get("_created", "")),
                            summary=newer.summary,
                        ))

        # Data gathering via state-doc flow (similar, parts, meta).
        # Edge resolution stays inline — it requires direct database
        # queries (inverse edges, explicit edge-tag lookup) that the
        # generic traverse action doesn't support.
        similar_refs: list[SimilarRef] = []
        meta_refs: dict[str, list[MetaRef]] = {}
        part_refs: list[PartRef] = []
        edge_refs: dict[str, list[EdgeRef]] = {}

        if offset == 0:
            if include_similar or include_meta or include_parts:
                from .tracing import get_tracer as _get_tracer
                with perf.timer("get_context", "read_flow", context_id=id), \
                     _get_tracer("keeper").start_as_current_span("get_context", attributes={"item_id": id}):
                    flow_result = self._run_read_flow(
                        "get",
                        {
                            "item_id": id,
                            "similar_limit": similar_limit if include_similar else 0,
                            "meta_limit": meta_limit if include_meta else 0,
                            "parts_limit": parts_limit if include_parts else 0,
                            "edges_limit": edges_limit,
                            "versions_limit": versions_limit if include_versions else 0,
                        },
                    )
                if flow_result.status == "done":
                    bindings = flow_result.bindings
                    if include_similar:
                        similar_refs = self._map_flow_similar(bindings.get("similar", {}))
                        similar_refs = similar_refs[:similar_limit]
                    if include_meta:
                        meta_refs = self._map_flow_meta(bindings.get("meta", {}))
                        # Cap each meta section to meta_limit
                        for key in list(meta_refs.keys()):
                            meta_refs[key] = meta_refs[key][:meta_limit]
                    if include_parts:
                        part_refs = self._map_flow_parts(bindings.get("parts", {}))
                        part_refs = part_refs[:parts_limit]
                    edge_refs = self._map_flow_edges(bindings.get("edges", {}))
                    # Cap each edge predicate to edges_limit
                    for key in list(edge_refs.keys()):
                        edge_refs[key] = edge_refs[key][:edges_limit]
                else:
                    logger.warning(
                        "get-context flow returned %s for %r: %s",
                        flow_result.status, id, flow_result.data,
                    )
                    # Fallback: inline edge resolution when flow fails
                    edge_refs = self._resolve_edge_refs(item, id)

        perf.record("get_context", "total", time.monotonic() - _ctx_t0,
                    context_id=id)
        return ItemContext(
            item=item,
            viewing_offset=offset,
            similar=similar_refs,
            meta=meta_refs,
            edges=edge_refs,
            parts=part_refs,
            prev=prev_refs,
            next=next_refs,
        )

    def resolve_version_offset(self, id: str, selector: int | None) -> Optional[int]:
        """Resolve a public version selector to a concrete offset.

        Public selector semantics:
        - None or 0: current version (offset 0)
        - N > 0: N versions back from current (offset N)
        - N < 0: Nth archived version from oldest (-1 oldest, -2 second-oldest)

        Returns:
            Resolved non-negative offset, or None if selector is out of range.
        """
        if selector is None or selector == 0:
            return 0
        if selector > 0:
            return selector

        doc_coll = self._resolve_doc_collection()
        archived_count = self._document_store.version_count(doc_coll, id)
        oldest_ordinal = -selector
        if oldest_ordinal < 1 or oldest_ordinal > archived_count:
            return None
        # oldest ordinal 1 maps to deepest offset (archived_count)
        return archived_count - oldest_ordinal + 1

    # ------------------------------------------------------------------
    # Agent prompts
    # ------------------------------------------------------------------

    def render_prompt(
        self,
        name: str,
        text: Optional[str] = None,
        *,
        id: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        tags: Optional[TagMap] = None,
        limit: int = 10,
        deep: bool = False,
        scope: Optional[str] = None,
        token_budget: Optional[int] = None,
    ) -> Optional[PromptResult]:
        """Render an agent prompt doc with injected context.

        Reads ``.prompt/agent/{name}`` from the store, extracts the
        ``## Prompt`` section, and assembles context.  The prompt text
        may contain ``{get}`` and ``{find}`` placeholders — the caller
        expands them with the rendered context and search results.
        Use ``{find:deep}`` to force deep tag-follow search regardless
        of the caller's ``deep`` parameter.

        Args:
            name: Prompt name (e.g. "reflect")
            text: Optional similarity key for search context
            id: Item ID for ``{get}`` context (default: "now")
            since: Time filter (ISO duration or date)
            until: Upper-bound time filter (ISO duration or date)
            tags: Tag filter for search results
            limit: Max search results
            deep: Follow tags from results to discover related items
            scope: ID glob to constrain search results
            token_budget: Explicit token budget (None = use template default)

        Returns:
            PromptResult with context, search_results, and prompt template,
            or None if the prompt doc doesn't exist.
        """
        from .analyzers import extract_prompt_section

        doc_id = f".prompt/agent/{name}"
        doc = self.get(doc_id)
        if doc is None:
            return None

        prompt_body = extract_prompt_section(doc.summary)
        if not prompt_body:
            # Fall back to full content if no ## Prompt section
            prompt_body = doc.summary

        # Check for state-doc flow reference in prompt tags.
        # When present, the flow's bindings replace the default get/find.
        state_doc = (doc.tags or {}).get("state")
        if state_doc:
            return self._render_prompt_via_flow(
                prompt_body, state_doc, text=text, id=id,
                since=since, until=until, tags=tags, scope=scope,
                token_budget=token_budget,
            )

        # Default path: hardcoded get + find
        context_id = id or "now"
        if context_id == "now":
            self.get_now()  # ensure now exists (auto-creates from bundled doc)
        ctx = self.get_context(context_id)

        # {find:deep} or {find:deep:N} in the template forces deep search
        if "{find:deep" in prompt_body:
            deep = True

        # Ensure retrieval limit is high enough to fill the token budget.
        # Deep items are rendered in a separate pass from leftover budget,
        # so the primary fetch size doesn't need to shrink for deep mode.
        tokens_per_item = 50
        effective_budget = token_budget or 4000
        fetch_limit = min(200, max(limit, effective_budget // tokens_per_item))

        # Search: find similar items with available filters
        search_results = None
        if text:
            search_results = self.find(
                query=text, tags=tags, since=since, until=until, limit=fetch_limit,
                deep=deep, scope=scope,
            )
        elif tags or since or until:
            search_results = self.find(
                similar_to=context_id, tags=tags, since=since, until=until,
                limit=fetch_limit, deep=deep, scope=scope,
            )

        return PromptResult(
            context=ctx,
            search_results=search_results,
            prompt=prompt_body,
            text=text,
            since=since,
            until=until,
            token_budget=token_budget,
        )

    def _render_prompt_via_flow(
        self,
        prompt_body: str,
        state_doc: str,
        *,
        text: Optional[str] = None,
        id: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        tags: Optional[TagMap] = None,
        scope: Optional[str] = None,
        token_budget: Optional[int] = None,
    ) -> PromptResult:
        """Run a state-doc flow and return a PromptResult with bindings."""
        params: dict[str, Any] = {}
        if text:
            params["query"] = text
            params["prompt"] = text
        if id:
            params["item_id"] = id
        if since:
            params["since"] = since
        if until:
            params["until"] = until
        if tags:
            params["tags"] = tags
        if scope:
            params["scope"] = scope

        flow_result = self._run_read_flow(state_doc, params, budget=10)

        return PromptResult(
            context=None,
            search_results=None,
            prompt=prompt_body,
            text=text,
            since=since,
            until=until,
            token_budget=token_budget,
            flow_bindings=flow_result.bindings if flow_result.status == "done" else None,
        )

    def list_prompts(self) -> list[PromptInfo]:
        """List available agent prompt docs.

        Returns:
            List of PromptInfo with name and summary for each
            ``.prompt/agent/*`` doc in the store.
        """
        prefix = ".prompt/agent/"
        items = self.list_items(
            prefix=prefix, include_hidden=True, limit=100,
        )
        result = []
        for item in items:
            name = item.id[len(prefix):]
            # Find first non-empty, non-heading line as description
            summary = ""
            for line in (item.summary or "").split("\n"):
                line = line.strip()
                if line and not line.startswith("#"):
                    summary = line
                    break
            result.append(PromptInfo(name=name, summary=summary))
        return result

    # ------------------------------------------------------------------
    # Similar items for display
    # ------------------------------------------------------------------

    def get_similar_for_display(
        self,
        id: str,
        *,
        limit: int = 3,
    ) -> list[Item]:
        """Find similar items for frontmatter display using stored embedding.

        Optimized for display: uses stored embedding (no re-embedding),
        filters to distinct base documents, excludes source document versions.

        Args:
            id: ID of item to find similar items for
            limit: Maximum results to return

        Returns:
            List of similar items, one per unique base document
        """
        chroma_coll = self._resolve_chroma_collection()

        # Get the stored embedding (no re-embedding)
        embedding = self._store.get_embedding(chroma_coll, id)
        if embedding is None:
            return []

        # Fetch more than needed to account for version/hidden filtering
        fetch_limit = limit * 5
        results = self._store.query_embedding(chroma_coll, embedding, limit=fetch_limit)

        # Convert to Items
        items = [r.to_item() for r in results]

        # Extract base ID of source document
        source_base_id = id.split("@v")[0] if "@v" in id else id

        # Filter to distinct base IDs, excluding source document and hidden notes
        seen_base_ids: set[str] = set()
        filtered: list[Item] = []
        for item in items:
            # Get base ID from tags or parse from ID
            base_id = item.tags.get("_base_id", item.id.split("@v")[0] if "@v" in item.id else item.id)

            # Skip versions of source document and hidden system notes
            if base_id == source_base_id or base_id.startswith("."):
                continue

            # Keep only first version of each document
            if base_id not in seen_base_ids:
                seen_base_ids.add(base_id)
                filtered.append(item)

                if len(filtered) >= limit:
                    break

        return filtered

    def get_version_offset(self, item: Item) -> int:
        """Get version offset (0=current, 1=previous, ...) for an item.

        Converts the internal version number (1=oldest, 2=next...) to the
        user-visible offset format (0=current, 1=previous, 2=two-ago...).

        Args:
            item: Item to get version offset for

        Returns:
            Version offset (0 for current version)
        """
        version_tag = item.tags.get("_version")
        if not version_tag:
            return 0  # Current version
        base_id = item.tags.get("_base_id", item.id)
        doc_coll = self._resolve_doc_collection()
        # Count versions >= this one to get the offset (handles gaps)
        internal_version = int(version_tag)
        return self._document_store.count_versions_from(
            doc_coll, base_id, internal_version
        )

    # ------------------------------------------------------------------
    # Meta-doc resolution
    # ------------------------------------------------------------------

    def resolve_meta(
        self,
        item_id: str,
        *,
        limit_per_doc: int = 3,
    ) -> dict[str, list[Item]]:
        """Resolve all .meta/* docs against an item's tags.

        Meta-docs are state docs evaluated via ``run_flow()``. Each rule
        typically uses ``find`` with ``similar_to`` for context-relevant
        ranking and ``tags`` for filtering.

        If a meta-doc contains an async action, the flow produces a cursor
        that is enqueued for daemon execution; partial results (from sync
        actions that completed before the async boundary) are returned.

        Falls back to the legacy line-based parser for old-format meta-docs
        during migration.

        Args:
            item_id: ID of the item whose tags provide context
            limit_per_doc: Max results per meta-doc

        Returns:
            Dict of {meta_name: [matching Items]}. Empty results omitted.
        """
        from .tracing import get_tracer

        doc_coll = self._resolve_doc_collection()
        tracer = get_tracer("keeper")

        # Find all .meta/* documents
        with tracer.start_as_current_span(
            "resolve_meta.load_docs",
            attributes={"item_id": item_id},
        ):
            meta_records = self._document_store.query_by_id_prefix(doc_coll, ".meta/")
        if not meta_records:
            return {}

        # Get current item's tags for context
        with tracer.start_as_current_span(
            "resolve_meta.load_item",
            attributes={"item_id": item_id},
        ):
            current = self.get(item_id)
        if current is None:
            return {}
        current_tags = current.tags

        # Build params for meta flows: item_id, limit, + all user tags
        flow_params: dict[str, Any] = {
            "item_id": item_id,
            "limit": limit_per_doc,
        }
        for k, v in current_tags.items():
            if not k.startswith("_"):
                flow_params[k] = v

        # Create env/loader/runner once for all meta-doc flows
        from .flow_env import LocalFlowEnvironment
        from .state_doc_runtime import make_action_runner, make_state_doc_loader
        env = LocalFlowEnvironment(self)
        loader = make_state_doc_loader(env)
        runner = make_action_runner(env, context_cache=self._context_cache)

        result: dict[str, list[Item]] = {}

        with tracer.start_as_current_span(
            "resolve_meta",
            attributes={"item_id": item_id, "meta_doc_count": len(meta_records)},
        ):
            for rec in meta_records:
                meta_id = rec.id
                short_name = meta_id.split("/", 1)[1] if "/" in meta_id else meta_id
                body = (rec.summary or "").strip()
                if not body:
                    continue

                with tracer.start_as_current_span(
                    "resolve_meta.doc",
                    attributes={"item_id": item_id, "meta_doc": short_name},
                ) as span:
                    matches = self._run_meta_flow(
                        short_name, body, flow_params, loader=loader, runner=runner,
                    )

                    # Legacy fallback: try old line-based format
                    if matches is None:
                        span.set_attribute("legacy", True)
                        matches = self._resolve_meta_legacy(
                            item_id, current_tags, body, limit_per_doc,
                        )
                    if matches is not None:
                        span.set_attribute("result_count", len(matches))

                if matches:
                    result[short_name] = matches

        return result

    def _run_meta_flow(
        self,
        name: str,
        body: str,
        params: dict[str, Any],
        *,
        loader: Any = None,
        runner: Any = None,
    ) -> list[Item] | None:
        """Run a meta-doc as a state-doc flow.

        Returns a list of matching Items, or None if the body does not
        parse as a state doc (triggering legacy fallback).
        """
        from .state_doc import parse_state_doc
        from .state_doc_runtime import run_flow

        try:
            doc = parse_state_doc(name, body)
        except (ValueError, RuntimeError):
            return None  # Not a state doc — legacy fallback

        # Create env/loader/runner if not provided (e.g. from inline meta)
        if loader is None or runner is None:
            from .flow_env import LocalFlowEnvironment
            from .state_doc_runtime import make_action_runner, make_state_doc_loader
            env = LocalFlowEnvironment(self)
            if loader is None:
                loader = make_state_doc_loader(env)
            if runner is None:
                runner = make_action_runner(env, context_cache=self._context_cache)

        # Provide the parsed doc directly via an inline loader
        base_loader = loader

        def _meta_loader(doc_name: str):
            if doc_name == name:
                return doc
            return base_loader(doc_name)

        flow_result = run_flow(
            name,
            params,
            budget=1,
            load_state_doc=_meta_loader,
            run_action=runner,
            foreground=True,
        )

        # If the flow hit an async action, enqueue cursor for daemon
        if flow_result.status == "async" and flow_result.cursor:
            try:
                self._enqueue_flow_cursor(
                    state=name,
                    cursor_token=flow_result.cursor,
                    params=params,
                )
            except Exception:
                logger.debug("Failed to enqueue async meta flow cursor for %s", name)

        # Collect Items from flow bindings
        return self._collect_items_from_bindings(flow_result.bindings)

    def _collect_items_from_bindings(
        self,
        bindings: dict[str, dict[str, Any]],
    ) -> list[Item]:
        """Extract Items from flow bindings, deduplicating by ID."""
        seen_ids: set[str] = set()
        items: list[Item] = []

        for _rule_id, binding in bindings.items():
            if not isinstance(binding, dict):
                continue
            results = binding.get("results")
            if not isinstance(results, list):
                continue
            for r in results:
                if not isinstance(r, dict):
                    continue
                item_id = r.get("id", "")
                if not item_id or item_id in seen_ids:
                    continue
                seen_ids.add(item_id)
                items.append(Item(
                    id=item_id,
                    summary=r.get("summary", ""),
                    tags=r.get("tags", {}),
                    score=r.get("score"),
                ))

        return items

    def _resolve_meta_legacy(
        self,
        item_id: str,
        current_tags: dict[str, str],
        body: str,
        limit: int,
    ) -> list[Item]:
        """Legacy meta-doc resolution using the line-based format.

        Kept for backward compatibility during migration. Will be
        removed after one version cycle.
        """
        query_lines, context_keys, prereq_keys = _parse_meta_doc(body)
        if not query_lines and not context_keys:
            return []

        return self._resolve_meta_queries_legacy(
            item_id, current_tags, query_lines, context_keys, prereq_keys, limit,
        )

    def _resolve_meta_queries_legacy(
        self,
        item_id: str,
        current_tags: dict[str, str],
        query_lines: list[dict[str, str]],
        context_keys: list[str],
        prereq_keys: list[str],
        limit: int,
    ) -> list[Item]:
        """Legacy shared resolution logic for persistent and inline metadocs."""
        # Check prerequisites: current item must have all required tags
        if prereq_keys:
            if not all(current_tags.get(k) for k in prereq_keys):
                return []

        # Get context values from current item's tags
        context_values: dict[str, str] = {}
        for key in context_keys:
            val = current_tags.get(key)
            if val and not key.startswith("_"):
                context_values[key] = val

        # Build expanded queries: cross-product of query lines x context values
        expanded: list[dict[str, str]] = []
        if context_values and query_lines:
            for query in query_lines:
                for ctx_key, ctx_val in context_values.items():
                    expanded.append({**query, ctx_key: ctx_val})
        elif context_values:
            for ctx_key, ctx_val in context_values.items():
                expanded.append({ctx_key: ctx_val})
        else:
            expanded = list(query_lines)

        # Run each expanded query, union results
        seen_ids: set[str] = set()
        matches: list[Item] = []
        for query in expanded:
            try:
                items = self.list_items(tags=query, limit=100)
            except (ValueError, Exception):
                continue
            for item in items:
                if item.id == item_id or _is_hidden(item) or item.id in seen_ids:
                    continue
                seen_ids.add(item.id)
                matches.append(item)

        if not matches:
            return []

        # Part-to-parent uplift
        direct_ids: set[str] = set()
        uplifted: list[Item] = []
        for item in matches:
            if not is_part_id(item.id):
                direct_ids.add(item.id)
        seen_parents: set[str] = set()
        for item in matches:
            if is_part_id(item.id):
                parent_id = item.tags.get("_base_id", item.id.split("@")[0])
                if parent_id in direct_ids or parent_id in seen_parents:
                    continue
                seen_parents.add(parent_id)
                uplifted.append(Item(
                    id=parent_id, summary=item.summary,
                    tags=item.tags, score=item.score,
                ))
            else:
                uplifted.append(item)

        if not uplifted:
            return []

        # Rank by similarity + recency decay
        uplifted = self._rank_by_relevance(self._resolve_chroma_collection(), item_id, uplifted)
        return uplifted[:limit]

    def resolve_inline_meta(
        self,
        item_id: str,
        queries: list[dict[str, str]],
        context_keys: list[str] | None = None,
        prereq_keys: list[str] | None = None,
        *,
        limit: int = 3,
    ) -> list[Item]:
        """Resolve an inline meta query against an item's tags.

        Builds a dynamic state doc from the query args and runs it as
        a flow. Falls back to legacy resolution if flow execution fails.

        Args:
            item_id: ID of the item whose tags provide context
            queries: List of tag-match dicts, each {key: value} for AND queries;
                     multiple dicts are OR (union)
            context_keys: Tag keys to expand from the current item's tags
            prereq_keys: Tag keys the current item must have (or return empty)
            limit: Max results

        Returns:
            List of matching Items, ranked by similarity.
        """
        current = self.get(item_id)
        if current is None:
            return []

        current_tags = current.tags

        # Build a dynamic state doc from the query args
        import yaml

        rules: list[dict[str, Any]] = []

        # Prerequisite guards: use has() to handle missing keys safely
        for key in (prereq_keys or []):
            rules.append({
                "when": f"!(has(params.{key}) && params.{key} != '')",
                "return": "done",
            })

        # Build find rules from query dicts
        effective_queries = list(queries)

        # Context expansion: if context_keys provided, create additional
        # queries from the current item's tag values
        if context_keys:
            for key in context_keys:
                val = current_tags.get(key)
                if val and not key.startswith("_"):
                    # Context-only: just the tag value as a query
                    if not queries:
                        effective_queries.append({key: val})
                    else:
                        # Cross-product: add context key to each query
                        for q in queries:
                            effective_queries.append({**q, key: val})

        for i, q in enumerate(effective_queries):
            rules.append({
                "id": f"q{i}",
                "do": "find",
                "with": {
                    "similar_to": item_id,
                    "tags": dict(q),
                    "limit": limit,
                },
            })

        if not rules:
            return []

        rules.append({"return": "done"})

        # Use 'sequence' when there are prereq guards (need short-circuit),
        # 'all' otherwise (independent find rules).
        match_mode = "sequence" if (prereq_keys or []) else "all"

        doc_body = yaml.dump(
            {"match": match_mode, "rules": rules},
            default_flow_style=False,
        )

        # Build params
        flow_params: dict[str, Any] = {
            "item_id": item_id,
            "limit": limit,
        }
        for k, v in current_tags.items():
            if not k.startswith("_"):
                flow_params[k] = v

        result = self._run_meta_flow("inline-meta", doc_body, flow_params)
        if result is not None:
            return result[:limit]

        # Legacy fallback
        return self._resolve_meta_queries_legacy(
            item_id, current_tags,
            queries, context_keys or [], prereq_keys or [], limit,
        )

    # ------------------------------------------------------------------
    # Legacy relevance ranking (used by legacy meta resolution)
    # ------------------------------------------------------------------

    def _rank_by_relevance(
        self,
        coll: str,
        anchor_id: str,
        candidates: list[Item],
    ) -> list[Item]:
        """Rank candidate items by similarity to anchor + recency decay.

        Uses stored embeddings — no re-embedding needed.
        Falls back to recency-only ranking if embeddings unavailable.

        Legacy: used only by ``_resolve_meta_queries_legacy()``. New
        meta-doc flows use ``find(similar_to=...)`` for native ranking.
        """
        if not candidates:
            return candidates

        try:
            candidate_ids = [c.id for c in candidates]
            all_ids = [anchor_id] + candidate_ids
            entries = self._store.get_entries_full(coll, all_ids)
        except Exception as e:
            logger.debug("Embedding lookup failed, falling back to recency: %s", e)
            return self._apply_recency_decay(candidates)

        emb_lookup: dict[str, list[float]] = {}
        for entry in entries:
            if entry.get("embedding") is not None:
                emb_lookup[entry["id"]] = entry["embedding"]

        anchor_emb = emb_lookup.get(anchor_id)
        if anchor_emb is None:
            return self._apply_recency_decay(candidates)

        def _cosine_sim(a: list[float], b: list[float]) -> float:
            dot = sum(x * y for x, y in zip(a, b))
            norm_a = math.sqrt(sum(x * x for x in a))
            norm_b = math.sqrt(sum(x * x for x in b))
            if norm_a == 0 or norm_b == 0:
                return 0.0
            return dot / (norm_a * norm_b)

        scored = []
        for item in candidates:
            emb = emb_lookup.get(item.id)
            sim = _cosine_sim(anchor_emb, emb) if emb is not None else 0.0
            scored.append(Item(id=item.id, summary=item.summary, tags=item.tags, score=sim))

        candidates = self._apply_recency_decay(scored)
        candidates.sort(key=lambda x: x.score or 0.0, reverse=True)
        return candidates
