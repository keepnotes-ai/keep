"""Remote Keeper — HTTP client for the keepnotes.ai API.

Implements KeeperProtocol by mapping method calls to REST endpoints.
Used when a [remote] section is configured in keep.toml or when
KEEPNOTES_API_URL and KEEPNOTES_API_KEY environment variables are set.
"""

import logging
import os
import re
from typing import Any, Iterator, Optional
from urllib.parse import quote

import httpx

# Project slug: must start with a letter, 2-63 chars, lowercase letters/numbers/hyphens
_SLUG_RE = re.compile(r'^[a-z][a-z0-9-]{0,61}[a-z0-9]$')

from .config import StoreConfig
from .document_store import VersionInfo
from .types import Item, ItemContext, SimilarRef, MetaRef, VersionRef, PartRef, TagMap, local_date

logger = logging.getLogger(__name__)

# Default timeout for API calls (seconds)
DEFAULT_TIMEOUT = 30.0


class RemoteKeeper:
    """Keeper backend that delegates to a remote keepnotes.ai API.

    Satisfies KeeperProtocol — the CLI uses it interchangeably with
    the local Keeper class.
    """

    def __init__(self, api_url: str, api_key: str, config: StoreConfig, *, project: Optional[str] = None):
        self.api_url = api_url.rstrip("/")
        self.api_key = api_key
        self._config = config

        # Project selection: explicit param > config > env var
        self.project = (
            project
            or (config.remote.project if config.remote else None)
            or os.environ.get("KEEPNOTES_PROJECT")
            or None
        )

        # Validate project slug format
        if self.project and not _SLUG_RE.match(self.project):
            raise ValueError(
                f"Invalid project slug '{self.project}'. "
                "Must start with a letter, 2-63 chars, lowercase letters/numbers/hyphens."
            )

        # Refuse non-HTTPS for remote APIs (bearer token would be sent in cleartext)
        if not self.api_url.startswith("https://"):
            from urllib.parse import urlparse
            host = urlparse(self.api_url).hostname or ""
            if host not in ("localhost", "127.0.0.1", "::1"):
                raise ValueError(
                    f"Remote API URL must use HTTPS (got {self.api_url}). "
                    "Use HTTPS to protect API credentials, or use localhost for local development."
                )

        headers: dict[str, str] = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        if self.project:
            headers["X-Project"] = self.project

        self._client = httpx.Client(
            base_url=self.api_url,
            headers=headers,
            timeout=DEFAULT_TIMEOUT,
        )

    @staticmethod
    def _q(id: str) -> str:
        """URL-encode an ID for safe use in URL path segments."""
        return quote(id, safe="")

    # -- HTTP helpers --

    def _get(self, path: str, **params: Any) -> dict:
        """GET request, return parsed JSON."""
        # Filter out None params
        filtered = {k: v for k, v in params.items() if v is not None}
        resp = self._client.get(path, params=filtered)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, json: dict) -> dict:
        """POST request with JSON body."""
        # Filter out None values
        filtered = {k: v for k, v in json.items() if v is not None}
        resp = self._client.post(path, json=filtered)
        resp.raise_for_status()
        return resp.json()

    def _put(self, path: str, json: dict) -> dict:
        """PUT request with JSON body."""
        filtered = {k: v for k, v in json.items() if v is not None}
        resp = self._client.put(path, json=filtered)
        resp.raise_for_status()
        return resp.json()

    def _patch(self, path: str, json: dict) -> dict:
        """PATCH request with JSON body."""
        resp = self._client.patch(path, json=json)
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path: str) -> dict:
        """DELETE request."""
        resp = self._client.delete(path)
        resp.raise_for_status()
        return resp.json()

    # -- Response conversion --

    @staticmethod
    def _to_item(data: dict) -> Item:
        """Convert API response dict to Item, with basic validation."""
        if not isinstance(data, dict):
            raise ValueError(f"Expected dict from API, got {type(data).__name__}")
        item_id = data.get("id")
        if not isinstance(item_id, str) or not item_id:
            raise ValueError(f"API response missing valid 'id' field: {data!r:.200}")
        tags = data.get("tags", {})
        if not isinstance(tags, dict):
            tags = {}
        # Ensure tag keys and values are strings
        tags = {str(k): str(v) for k, v in tags.items()}
        if data.get("created_at"):
            tags.setdefault("_created", str(data["created_at"]))
        if data.get("updated_at"):
            tags.setdefault("_updated", str(data["updated_at"]))
        summary = data.get("summary", "")
        if not isinstance(summary, str):
            summary = str(summary)
        score = data.get("score")
        if score is not None:
            try:
                score = float(score)
            except (TypeError, ValueError):
                score = None
        return Item(
            id=item_id,
            summary=summary,
            tags=tags,
            score=score,
        )

    @staticmethod
    def _to_items(data: dict) -> list[Item]:
        """Convert API list response to list of Items."""
        if not isinstance(data, dict):
            raise ValueError(f"Expected dict from API, got {type(data).__name__}")
        items = data.get("notes", data.get("items", []))
        if not isinstance(items, list):
            raise ValueError(f"Expected 'notes' list from API, got {type(items).__name__}")
        return [RemoteKeeper._to_item(item) for item in items]

    @staticmethod
    def _to_version_info(data: dict) -> VersionInfo:
        """Convert API response dict to VersionInfo."""
        return VersionInfo(
            version=data["version"],
            summary=data.get("summary", ""),
            tags=data.get("tags", {}),
            created_at=data.get("created_at", ""),
            content_hash=data.get("content_hash"),
        )

    # -- Write operations --

    def put(
        self,
        content: Optional[str] = None,
        *,
        uri: Optional[str] = None,
        id: Optional[str] = None,
        summary: Optional[str] = None,
        tags: Optional[TagMap] = None,
        created_at: Optional[str] = None,
    ) -> Item:
        resp = self._post("/v1/notes", json={
            "content": content,
            "uri": uri,
            "id": id,
            "tags": tags,
            "summary": summary,
            "created_at": created_at,
        })
        return self._to_item(resp)

    def set_now(
        self,
        content: str,
        *,
        scope: Optional[str] = None,
        tags: Optional[TagMap] = None,
    ) -> Item:
        params = {"scope": scope} if scope else {}
        filtered = {k: v for k, v in {"content": content, "tags": tags}.items() if v is not None}
        resp = self._client.put("/v1/now", params=params, json=filtered)
        resp.raise_for_status()
        return self._to_item(resp.json())

    def tag(
        self,
        id: str,
        tags: Optional[TagMap] = None,
    ) -> Optional[Item]:
        if tags is None:
            return self.get(id)
        resp = self._patch(f"/v1/notes/{self._q(id)}/tags", json={
            "set": {k: v for k, v in tags.items() if v},
            "remove": [k for k, v in tags.items() if not v],
        })
        return self._to_item(resp)

    def delete(
        self,
        id: str,
        *,
        delete_versions: bool = True,
    ) -> bool:
        resp = self._delete(f"/v1/notes/{self._q(id)}")
        return resp.get("deleted", False)

    def revert(self, id: str) -> Optional[Item]:
        resp = self._post(f"/v1/notes/{self._q(id)}/revert", json={})
        if resp.get("deleted"):
            return None
        return self._to_item(resp)

    def move(
        self,
        name: str,
        *,
        source_id: str = "now",
        tags: Optional[TagMap] = None,
        only_current: bool = False,
    ) -> Item:
        resp = self._post("/v1/move", json={
            "target": name,
            "source": source_id,
            "tags": tags,
            "only_current": only_current,
        })
        return self._to_item(resp)

    # -- Query operations --

    def find(
        self,
        query: Optional[str] = None,
        *,
        tags: Optional[TagMap] = None,
        similar_to: Optional[str] = None,
        limit: int = 10,
        since: Optional[str] = None,
        until: Optional[str] = None,
        include_self: bool = False,
        include_hidden: bool = False,
        deep: bool = False,
    ) -> list[Item]:
        from .api import FindResults
        resp = self._post("/v1/search", json={
            "query": query,
            "similar_to": similar_to,
            "tags": tags,
            "limit": limit,
            "since": since,
            "until": until,
            "include_self": include_self or None,
            "include_hidden": include_hidden or None,
            "deep": deep or None,
        })
        items = self._to_items(resp)
        # Parse deep groups from API response if present
        deep_groups: dict[str, list[Item]] = {}
        for raw_group in resp.get("deep_groups", []):
            pid = raw_group.get("id", "")
            if pid and "items" in raw_group:
                deep_groups[pid] = [self._to_item(i) for i in raw_group["items"]]
        return FindResults(items, deep_groups=deep_groups)

    def get_similar_for_display(
        self,
        id: str,
        *,
        limit: int = 3,
    ) -> list[Item]:
        resp = self._get(f"/v1/notes/{self._q(id)}/similar", limit=limit)
        return self._to_items(resp)

    def list_tags(
        self,
        key: Optional[str] = None,
    ) -> list[str]:
        if key:
            resp = self._get(f"/v1/tags/{self._q(key)}")
        else:
            resp = self._get("/v1/tags")
        return resp.get("values", [])

    def resolve_meta(
        self,
        item_id: str,
        *,
        limit_per_doc: int = 3,
    ) -> dict[str, list[Item]]:
        resp = self._get(f"/v1/notes/{self._q(item_id)}/meta", limit=limit_per_doc)
        result: dict[str, list[Item]] = {}
        for name, items_data in resp.get("sections", {}).items():
            result[name] = [self._to_item(i) for i in items_data]
        return result

    def resolve_inline_meta(
        self,
        item_id: str,
        queries: list[dict[str, str]],
        context_keys: list[str] | None = None,
        prereq_keys: list[str] | None = None,
        *,
        limit: int = 3,
    ) -> list[Item]:
        resp = self._post(f"/v1/notes/{self._q(item_id)}/resolve", json={
            "queries": queries,
            "context_keys": context_keys,
            "prerequisites": prereq_keys,
            "limit": limit,
        })
        return self._to_items(resp)

    def list_items(
        self,
        *,
        prefix: Optional[str] = None,
        tags: Optional[TagMap] = None,
        tag_keys: Optional[list[str]] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        order_by: str = "updated",
        include_hidden: bool = False,
        include_history: bool = False,
        limit: int = 10,
    ) -> list[Item]:
        # Build tag query params: key=value for exact match, key-only for existence
        tag_params: list[str] = []
        if tags:
            for k, v in tags.items():
                tag_params.append(f"{k}={v}")
        if tag_keys:
            tag_params.extend(tag_keys)

        params: dict = {
            "limit": limit,
            "since": since,
            "until": until,
            "order_by": order_by,
            "include_history": include_history or None,
            "include_hidden": include_hidden or None,
            "prefix": prefix,
        }
        if tag_params:
            params["tag"] = tag_params

        resp = self._get("/v1/notes", **params)
        return self._to_items(resp)

    # -- Display context --

    def get_context(
        self,
        id: str,
        *,
        version: int | None = None,
        similar_limit: int = 3,
        meta_limit: int = 3,
        include_similar: bool = True,
        include_meta: bool = True,
        include_parts: bool = True,
        include_versions: bool = True,
    ) -> ItemContext | None:
        """Assemble display context from individual remote API calls."""
        offset = version or 0
        if offset > 0:
            item = self.get_version(id, offset)
        else:
            item = self.get(id)
        if item is None:
            return None

        # Version navigation
        prev_refs: list[VersionRef] = []
        next_refs: list[VersionRef] = []
        if include_versions:
            nav = self.get_version_nav(id, version)
            for i, v in enumerate(nav.get("prev", [])):
                prev_refs.append(VersionRef(
                    offset=offset + i + 1,
                    date=local_date(v.tags.get("_created") or v.created_at or ""),
                    summary=v.summary,
                ))
            for i, v in enumerate(nav.get("next", [])):
                next_refs.append(VersionRef(
                    offset=offset - i - 1,
                    date=local_date(v.tags.get("_created") or v.created_at or ""),
                    summary=v.summary,
                ))

        # Similar items (current version only)
        similar_refs: list[SimilarRef] = []
        if include_similar and offset == 0:
            raw = self.get_similar_for_display(id, limit=similar_limit)
            for s in raw:
                s_offset = self.get_version_offset(s)
                similar_refs.append(SimilarRef(
                    id=s.tags.get("_base_id", s.id),
                    offset=s_offset,
                    score=s.score,
                    date=local_date(
                        s.tags.get("_updated") or s.tags.get("_created", "")
                    ),
                    summary=s.summary,
                ))

        # Meta-doc sections (current version only)
        meta_refs: dict[str, list[MetaRef]] = {}
        if include_meta and offset == 0:
            raw_meta = self.resolve_meta(id, limit_per_doc=meta_limit)
            for name, meta_items in raw_meta.items():
                meta_refs[name] = [
                    MetaRef(id=mi.id, summary=mi.summary)
                    for mi in meta_items
                ]

        # Parts — remote API doesn't expose list_parts yet
        part_refs: list[PartRef] = []

        return ItemContext(
            item=item,
            viewing_offset=offset,
            similar=similar_refs,
            meta=meta_refs,
            parts=part_refs,
            prev=prev_refs,
            next=next_refs,
        )

    # -- Direct access --

    def get(self, id: str) -> Optional[Item]:
        try:
            resp = self._get(f"/v1/notes/{self._q(id)}")
            return self._to_item(resp)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    def get_now(self, *, scope: Optional[str] = None) -> Item:
        if scope:
            resp = self._get("/v1/now", scope=scope)
        else:
            resp = self._get("/v1/now")
        return self._to_item(resp)

    def get_version(
        self,
        id: str,
        offset: int = 0,
    ) -> Optional[Item]:
        try:
            resp = self._get(f"/v1/notes/{self._q(id)}/versions/{offset}")
            return self._to_item(resp)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    def list_versions(
        self,
        id: str,
        limit: int = 10,
    ) -> list[VersionInfo]:
        resp = self._get(f"/v1/notes/{self._q(id)}/versions", limit=limit)
        return [self._to_version_info(v) for v in resp.get("versions", [])]

    def list_versions_around(
        self,
        id: str,
        version: int,
        radius: int = 2,
    ) -> list[VersionInfo]:
        # NOTE: requires server-side support for `around` and `radius`
        # query params.  Older servers will silently ignore them and
        # return default list_versions output (newest first, limited).
        resp = self._get(
            f"/v1/notes/{self._q(id)}/versions",
            around=version, radius=radius,
        )
        return [self._to_version_info(v) for v in resp.get("versions", [])]

    def get_version_nav(
        self,
        id: str,
        current_version: Optional[int] = None,
        limit: int = 3,
    ) -> dict:
        resp = self._get(
            f"/v1/notes/{self._q(id)}/versions/nav",
            current_version=current_version,
            limit=limit,
        )
        result: dict[str, list[VersionInfo]] = {}
        for key in ("prev", "next"):
            if key in resp:
                result[key] = [self._to_version_info(v) for v in resp[key]]
        return result

    def get_version_offset(self, item: Item) -> int:
        resp = self._get(f"/v1/notes/{self._q(item.id)}/version-offset")
        return resp.get("offset", 0)

    def exists(self, id: str) -> bool:
        try:
            self._get(f"/v1/notes/{self._q(id)}")
            return True
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return False
            raise

    # -- Collection management --

    def list_collections(self) -> list[str]:
        resp = self._get("/v1/collections")
        return resp.get("collections", [])

    def count(self) -> int:
        resp = self._get("/v1/count")
        return resp.get("count", 0)

    def export_iter(self, *, include_system: bool = True) -> Iterator[dict]:
        """Stream export is not yet supported for hosted stores."""
        raise NotImplementedError("Export/import not yet supported for hosted stores")

    def export_data(self, *, include_system: bool = True) -> dict:
        """Export is not yet supported for hosted stores."""
        raise NotImplementedError("Export/import not yet supported for hosted stores")

    def import_data(self, data: dict, *, mode: str = "merge") -> dict:
        """Import is not yet supported for hosted stores."""
        raise NotImplementedError("Export/import not yet supported for hosted stores")

    # -- Continuation API --

    def continue_flow(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("continue payload must be a JSON object")
        resp = self._post("/v1/continue", json=payload)
        if not isinstance(resp, dict):
            raise ValueError("Remote /v1/continue response must be a JSON object")
        return resp

    def continue_run_work(self, cursor: str, work_id: str) -> dict[str, Any]:
        if not cursor or not work_id:
            raise ValueError("cursor and work_id are required")
        resp = self._post(
            "/v1/continue/work",
            json={"cursor": str(cursor), "work_id": str(work_id)},
        )
        if not isinstance(resp, dict):
            raise ValueError("Remote /v1/continue/work response must be a JSON object")
        return resp

    def close(self) -> None:
        self._client.close()
