"""
Action registry and base contracts for state-doc style actions.
"""

from __future__ import annotations

import importlib
import pkgutil
import re
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "Action",
    "ActionContext",
    "action",
    "get_action",
    "list_actions",
    "item_to_result",
    "coerce_item_id",
]


@runtime_checkable
class Action(Protocol):
    def run(self, params: dict[str, Any], context: "ActionContext") -> dict[str, Any]: ...


@runtime_checkable
class ActionContext(Protocol):
    item_id: str | None
    item_content: str | None

    def get(self, id: str) -> Any | None: ...

    def find(
        self,
        query: str | None = None,
        *,
        tags: dict[str, Any] | None = None,
        similar_to: str | None = None,
        limit: int = 10,
        since: str | None = None,
        until: str | None = None,
        include_hidden: bool = False,
    ) -> list[Any]: ...

    def list_items(
        self,
        *,
        prefix: str | None = None,
        tags: dict[str, Any] | None = None,
        since: str | None = None,
        until: str | None = None,
        order_by: str = "updated",
        include_hidden: bool = False,
        limit: int = 10,
    ) -> list[Any]: ...

    def get_document(self, id: str) -> Any | None: ...
    def resolve_meta(self, id: str, limit_per_doc: int = 3) -> dict[str, list[Any]]: ...
    def traverse(self, source_ids: list[str], *, limit: int = 5) -> dict[str, list[Any]]: ...
    def resolve_provider(self, kind: str, name: str | None = None) -> Any: ...


_ACTION_REGISTRY: dict[str, type] = {}
_ACTIONS_DISCOVERED = False


def _snake_case(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def action(cls: type | None = None, *, name: str | None = None):
    def _register(target: type) -> type:
        action_name = str(name or _snake_case(target.__name__)).strip()
        if not action_name:
            raise ValueError("action name cannot be empty")
        _ACTION_REGISTRY[action_name] = target
        return target

    if cls is None:
        return _register
    return _register(cls)


def _discover_actions() -> None:
    global _ACTIONS_DISCOVERED
    if _ACTIONS_DISCOVERED:
        return
    for module in pkgutil.iter_modules(__path__):  # type: ignore[name-defined]
        mod_name = module.name
        if mod_name.startswith("_"):
            continue
        importlib.import_module(f"{__name__}.{mod_name}")
    _ACTIONS_DISCOVERED = True


def get_action(name: str) -> Action:
    _discover_actions()
    key = str(name or "").strip()
    cls = _ACTION_REGISTRY.get(key)
    if cls is None:
        known = ", ".join(sorted(_ACTION_REGISTRY.keys()))
        raise ValueError(f"unknown action: {key!r} (known: {known})")
    inst = cls()
    if not isinstance(inst, Action):
        raise TypeError(f"action {key!r} does not implement Action protocol")
    return inst


def list_actions() -> list[str]:
    _discover_actions()
    return sorted(_ACTION_REGISTRY.keys())


def coerce_item_id(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    if isinstance(value, dict):
        raw = value.get("id")
        if raw is None:
            return None
        return str(raw).strip() or None
    raw = getattr(value, "id", None)
    if raw is None:
        return None
    return str(raw).strip() or None


def item_to_result(item: Any) -> dict[str, Any]:
    tags = getattr(item, "tags", None)
    score = getattr(item, "score", None)
    out = {
        "id": str(getattr(item, "id", "")),
        "summary": str(getattr(item, "summary", "")),
        "tags": dict(tags) if isinstance(tags, dict) else {},
        "score": float(score) if isinstance(score, (int, float)) else None,
    }
    return out
