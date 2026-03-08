"""Continuation work executor registry and local default executor.

This module isolates runner/provider execution from continuation state control.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Protocol

from .actions import get_action
from .continuation_env import ContinuationRuntimeEnv
from .processors import process_summarize


@dataclass
class RunnerExecution:
    """Result of executing a work item runner."""

    outputs: dict[str, Any]
    executor_id: str
    quality: dict[str, Any]


@dataclass
class _ActionItem:
    """Action-facing item view with best-available content and source URI."""

    id: str
    summary: str
    tags: dict[str, Any]
    score: float | None = None
    changed: bool | None = None
    content: str | None = None
    uri: str | None = None


ProviderResolver = Callable[["LocalWorkExecutor", str | None, dict[str, Any] | None], tuple[Any, str]]
RunnerHandler = Callable[["LocalWorkExecutor", dict[str, Any], dict[str, Any]], RunnerExecution]


class WorkExecutor(Protocol):
    """Execution contract consumed by continuation engine."""

    def execute(self, payload: dict[str, Any]) -> RunnerExecution: ...


class ContinuationExecutorRegistry:
    """Registry mapping runner types and provider kinds to handlers."""

    def __init__(self) -> None:
        self._provider_resolvers: dict[str, ProviderResolver] = {}
        self._runner_handlers: dict[str, RunnerHandler] = {}

    def register_provider_kind(self, kind: str, resolver: ProviderResolver) -> None:
        self._provider_resolvers[str(kind)] = resolver

    def register_runner(self, runner_type: str, handler: RunnerHandler) -> None:
        self._runner_handlers[str(runner_type)] = handler

    def get_provider_resolver(self, kind: str) -> ProviderResolver:
        if kind not in self._provider_resolvers:
            raise ValueError(f"unsupported runner.provider.kind: {kind}")
        return self._provider_resolvers[kind]

    def get_runner(self, runner_type: str) -> RunnerHandler:
        if runner_type not in self._runner_handlers:
            raise ValueError(f"unsupported runner type: {runner_type}")
        return self._runner_handlers[runner_type]


DEFAULT_CONTINUATION_EXECUTOR_REGISTRY = ContinuationExecutorRegistry()


class LocalWorkExecutor:
    """Executes continuation work items using local providers."""

    RESOLVABLE_PAYLOAD_ROOT_KEYS = {
        "content",
        "item_id",
        "collection",
        "tags",
        "summary",
        "context",
        "metadata",
        "input",
    }

    def __init__(
        self,
        env: ContinuationRuntimeEnv,
        *,
        registry: ContinuationExecutorRegistry | None = None,
    ) -> None:
        self._env = env
        self._registry = registry or DEFAULT_CONTINUATION_EXECUTOR_REGISTRY

    @staticmethod
    def resolve_value(value: Any, payload: dict[str, Any]) -> Any:
        if isinstance(value, str) and value.startswith("$"):
            return LocalWorkExecutor._resolve_payload_ref(value[1:], payload)
        if isinstance(value, list):
            return [LocalWorkExecutor.resolve_value(v, payload) for v in value]
        if isinstance(value, dict):
            return {str(k): LocalWorkExecutor.resolve_value(v, payload) for k, v in value.items()}
        return value

    @staticmethod
    def _resolve_payload_ref(ref: str, payload: dict[str, Any]) -> Any:
        path = str(ref or "").strip()
        if not path:
            raise ValueError("runner variable reference cannot be empty")

        root, *rest = path.split(".")
        if root not in LocalWorkExecutor.RESOLVABLE_PAYLOAD_ROOT_KEYS:
            raise ValueError(f"runner variable root is not allowed: {root}")

        current: Any = payload.get(root)
        for part in rest:
            if not isinstance(current, dict):
                raise ValueError(f"runner variable path is not an object at: {part}")
            if part not in current:
                return None
            current = current.get(part)
        return current

    def resolve_provider(
        self,
        runner: dict[str, Any],
        *,
        default_kind: str,
    ) -> tuple[Any, str]:
        spec = runner.get("provider")
        if not isinstance(spec, dict):
            spec = {}
        provider_kind = str(spec.get("kind") or runner.get("provider_kind") or default_kind)
        provider_name = spec.get("name")
        provider_params = spec.get("params")
        if provider_params is not None and not isinstance(provider_params, dict):
            raise ValueError("runner.provider.params must be an object")
        resolver = self._registry.get_provider_resolver(provider_kind)
        return resolver(self, provider_name, provider_params)

    @staticmethod
    def default_quality() -> dict[str, Any]:
        return {
            "confidence": 1.0,
            "passed_gates": True,
            "fail_reasons": [],
        }

    def execute(self, payload: dict[str, Any]) -> RunnerExecution:
        runner = payload.get("runner") or {}
        if not isinstance(runner, dict):
            raise ValueError("work item runner must be an object")
        runner_type = str(runner.get("type") or "").strip()
        if not runner_type:
            raise ValueError("runner.type is required for local run_work execution")
        if runner_type == "action":
            return _run_action(self, payload, runner, action_name=str(runner.get("name") or "").strip())
        if runner_type.startswith("action."):
            return _run_action(self, payload, runner, action_name=runner_type[7:])
        handler = self._registry.get_runner(runner_type)
        return handler(self, payload, runner)


class _ActionRunnerContext:
    """Read-only action context backed by continuation runtime adapters."""

    def __init__(self, executor: LocalWorkExecutor, payload: dict[str, Any]) -> None:
        """Bind runtime environment and current work payload."""
        self._executor = executor
        self._env = executor._env
        self._cache: dict[str, Any] = {}
        item_id = payload.get("item_id")
        self.item_id = str(item_id) if item_id is not None else None
        content = payload.get("content")
        self.item_content = str(content) if content is not None else None

    def get(self, id: str) -> Any | None:
        """Return an action item, enriching URI-backed notes with fetched content."""
        key = str(id).strip()
        if not key:
            return None
        if key in self._cache:
            return self._cache[key]
        base = self._env.get(key)
        if base is None:
            self._cache[key] = None
            return None

        tags_raw = getattr(base, "tags", None)
        tags = dict(tags_raw) if isinstance(tags_raw, dict) else {}
        source = str(tags.get("_source") or "").strip().lower()
        source_uri = str(tags.get("_source_uri") or "").strip()
        uri = source_uri or (key if source == "uri" else "")
        content: str | None = None
        if uri:
            try:
                provider = self.resolve_provider("document")
                fetch = getattr(provider, "fetch", None)
                if callable(fetch):
                    doc = fetch(uri)
                    raw_content = getattr(doc, "content", None)
                    if raw_content is not None:
                        content = str(raw_content)
            except Exception:
                content = None

        if content is None and self.item_id and key == self.item_id and self.item_content is not None:
            content = str(self.item_content)
        if content is None:
            content = str(getattr(base, "summary", "") or "")

        item = _ActionItem(
            id=str(getattr(base, "id", key) or key),
            summary=str(getattr(base, "summary", "") or ""),
            tags=tags,
            score=(float(getattr(base, "score")) if isinstance(getattr(base, "score", None), (int, float)) else None),
            changed=(bool(getattr(base, "changed")) if getattr(base, "changed", None) is not None else None),
            content=content,
            uri=(uri or None),
        )
        self._cache[key] = item
        return item

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
    ) -> list[Any]:
        return self._env.find(
            query,
            tags=tags,
            similar_to=similar_to,
            limit=limit,
            since=since,
            until=until,
            include_hidden=include_hidden,
            deep=False,
        )

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
    ) -> list[Any]:
        return self._env.list_items(
            prefix=prefix,
            tags=tags,
            since=since,
            until=until,
            order_by=order_by,
            include_hidden=include_hidden,
            limit=limit,
        )

    def get_document(self, id: str) -> Any | None:
        return self._env.get_document(id)

    def resolve_meta(self, id: str, limit_per_doc: int = 3) -> dict[str, list[Any]]:
        return self._env.resolve_meta(id, limit_per_doc=limit_per_doc)

    def traverse(self, source_ids: list[str], *, limit: int = 5) -> dict[str, list[Any]]:
        return self._env.traverse_related(source_ids, limit_per_source=limit)

    def resolve_provider(self, kind: str, name: str | None = None) -> Any:
        resolver = self._executor._registry.get_provider_resolver(str(kind))
        provider, _ = resolver(self._executor, name, None)
        return provider


def _run_action(
    executor: LocalWorkExecutor,
    payload: dict[str, Any],
    runner: dict[str, Any],
    *,
    action_name: str,
) -> RunnerExecution:
    """Execute a registered state action and normalize its outputs."""
    if not action_name:
        raise ValueError("runner action requires name")
    raw_params = runner.get("params")
    if raw_params is None:
        raw_params = {}
    if not isinstance(raw_params, dict):
        raise ValueError("runner action params must be an object")
    params = executor.resolve_value(raw_params, payload)
    if not isinstance(params, dict):
        raise ValueError("resolved action params must be an object")

    action = get_action(action_name)
    context = _ActionRunnerContext(executor, payload)
    outputs = action.run(params, context)
    if not isinstance(outputs, dict):
        raise ValueError(f"action {action_name!r} must return an object")
    return RunnerExecution(
        outputs={str(k): v for k, v in outputs.items()},
        executor_id=str(runner.get("executor_id") or f"action.{action_name}"),
        quality=LocalWorkExecutor.default_quality(),
    )


def _resolve_summarization_provider(
    executor: LocalWorkExecutor, name: str | None, params: dict[str, Any] | None,
) -> tuple[Any, str]:
    if name is None and params is None:
        return executor._env.get_default_summarization_provider(), "summarization.default"
    if name is None:
        raise ValueError("runner.provider.name is required when provider params are provided")
    from .providers.base import get_registry

    registry = get_registry()
    return registry.create_summarization(str(name), params), str(name)


def _resolve_document_provider(
    executor: LocalWorkExecutor, name: str | None, params: dict[str, Any] | None,
) -> tuple[Any, str]:
    """Resolve a document provider for URI-backed content refresh."""
    if name is None and params is None:
        return executor._env.get_default_document_provider(), "document.default"
    if name is None:
        raise ValueError("runner.provider.name is required when document provider params are provided")
    from .providers.base import get_registry

    registry = get_registry()
    return registry.create_document(str(name), params), str(name)


def _resolve_tagging_provider(
    executor: LocalWorkExecutor, name: str | None, params: dict[str, Any] | None,
) -> tuple[Any, str]:
    if name is None and params is None:
        return executor._env.get_default_tagging_provider(), "tagging.default"
    from .providers.base import get_registry

    registry = get_registry()
    if name is None:
        raise ValueError("runner.provider.name is required when tagging provider params are provided")
    return registry.create_tagging(str(name), params), str(name)


def _resolve_analyzer_provider(
    executor: LocalWorkExecutor, name: str | None, params: dict[str, Any] | None,
) -> tuple[Any, str]:
    if name is None and params is None:
        return executor._env.get_default_analyzer_provider(), "analyzer.default"
    if name is None:
        raise ValueError("runner.provider.name is required when analyzer provider params are provided")
    from .providers.base import get_registry

    registry = get_registry()
    return registry.create_analyzer(str(name), params), str(name)


def _resolve_content_extractor_provider(
    executor: LocalWorkExecutor, name: str | None, params: dict[str, Any] | None,
) -> tuple[Any, str]:
    if name is None and params is None:
        return executor._env.get_default_content_extractor_provider(), "content_extractor.default"
    if name is None:
        raise ValueError("runner.provider.name is required when content extractor params are provided")
    from .providers.base import get_registry

    registry = get_registry()
    return registry.create_content_extractor(str(name), params), str(name)


def _run_provider_summarize(
    executor: LocalWorkExecutor, payload: dict[str, Any], runner: dict[str, Any],
) -> RunnerExecution:
    content = payload.get("content")
    if not content:
        raise ValueError("runner provider.summarize requires work input content")

    context = runner.get("context")
    if isinstance(context, str):
        resolved_context = executor.resolve_value(context, payload)
        context = None if resolved_context is None else str(resolved_context)
    elif context is not None:
        context = str(context)

    provider, provider_id = executor.resolve_provider(runner, default_kind="summarization")
    result = process_summarize(
        str(content),
        summarization_provider=provider,
        context=context,
        system_prompt_override=runner.get("system_prompt"),
    )
    output_key = str(runner.get("output_key") or "summary")
    return RunnerExecution(
        outputs={output_key: result.summary or ""},
        executor_id=str(runner.get("executor_id") or provider_id),
        quality=LocalWorkExecutor.default_quality(),
    )


def _run_provider_tag(
    executor: LocalWorkExecutor, payload: dict[str, Any], runner: dict[str, Any],
) -> RunnerExecution:
    content = payload.get("content")
    if not content:
        raise ValueError("runner provider.tag requires work input content")
    provider, provider_id = executor.resolve_provider(runner, default_kind="tagging")
    tag_method = getattr(provider, "tag", None)
    if not callable(tag_method):
        raise ValueError("tagging provider does not expose tag(content)")
    tags = tag_method(str(content))
    if not isinstance(tags, dict):
        raise ValueError("tagging provider must return an object")
    output_key = str(runner.get("output_key") or "tags")
    normalized = {str(k): str(v) for k, v in tags.items()}
    return RunnerExecution(
        outputs={output_key: normalized},
        executor_id=str(runner.get("executor_id") or provider_id),
        quality=LocalWorkExecutor.default_quality(),
    )


def _run_provider_generate_json(
    executor: LocalWorkExecutor, payload: dict[str, Any], runner: dict[str, Any],
) -> RunnerExecution:
    provider, provider_id = executor.resolve_provider(runner, default_kind="summarization")
    generate = getattr(provider, "generate", None)
    if not callable(generate):
        raise ValueError("provider.generate_json requires provider.generate(system, user, ...)")

    resolved_system = executor.resolve_value(runner.get("system") or "", payload)
    if resolved_system is None:
        system = ""
    elif isinstance(resolved_system, (dict, list)):
        system = json.dumps(resolved_system, ensure_ascii=False)
    else:
        system = str(resolved_system)

    resolved_user = executor.resolve_value(runner.get("user") or payload.get("content") or "", payload)
    if resolved_user is None:
        user = ""
    elif isinstance(resolved_user, (dict, list)):
        user = json.dumps(resolved_user, ensure_ascii=False)
    else:
        user = str(resolved_user)
    max_tokens = int(runner.get("max_tokens") or 4096)
    raw = generate(system, user, max_tokens=max_tokens)
    if raw is None:
        raise ValueError("provider.generate_json returned no content")

    try:
        parsed = json.loads(raw)
    except Exception:
        from .providers.base import parse_tag_json

        parsed = parse_tag_json(str(raw))

    if isinstance(parsed, dict):
        outputs = {str(k): v for k, v in parsed.items()}
    else:
        output_key = str(runner.get("output_key") or "result")
        outputs = {output_key: parsed}
    return RunnerExecution(
        outputs=outputs,
        executor_id=str(runner.get("executor_id") or provider_id),
        quality=LocalWorkExecutor.default_quality(),
    )


def _run_echo(
    executor: LocalWorkExecutor, payload: dict[str, Any], runner: dict[str, Any],
) -> RunnerExecution:
    outputs = runner.get("outputs")
    if isinstance(outputs, dict):
        resolved = {str(k): executor.resolve_value(v, payload) for k, v in outputs.items()}
    else:
        source_key = str(runner.get("source") or "content")
        output_key = str(runner.get("output_key") or "value")
        resolved = {output_key: payload.get(source_key)}
    return RunnerExecution(
        outputs=resolved,
        executor_id=str(runner.get("executor_id") or "echo"),
        quality=LocalWorkExecutor.default_quality(),
    )


def _run_local_task(
    executor: LocalWorkExecutor, payload: dict[str, Any], runner: dict[str, Any],
) -> RunnerExecution:
    task_type = str(runner.get("task_type") or payload.get("task_type") or "").strip()
    if not task_type:
        raise ValueError("runner local.task requires task_type")
    item_id = str(payload.get("item_id") or "").strip()
    if not item_id:
        raise ValueError("runner local.task requires item_id")
    collection = str(payload.get("collection") or executor._env.resolve_doc_collection())
    content = str(payload.get("content") or "")
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    outcome = executor._env.run_local_task_workflow(
        task_type=task_type,
        item_id=item_id,
        collection=collection,
        content=content,
        metadata=metadata,
    )
    status = str((outcome or {}).get("status") or "applied")
    details = (outcome or {}).get("details")
    if not isinstance(details, dict):
        details = {}
    return RunnerExecution(
        outputs={"status": status, "details": details},
        executor_id=str(runner.get("executor_id") or f"local.task.{task_type}"),
        quality=LocalWorkExecutor.default_quality(),
    )


DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_provider_kind("summarization", _resolve_summarization_provider)
DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_provider_kind("document", _resolve_document_provider)
DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_provider_kind("tagging", _resolve_tagging_provider)
DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_provider_kind("analyzer", _resolve_analyzer_provider)
DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_provider_kind("content_extractor", _resolve_content_extractor_provider)

DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_runner("provider.summarize", _run_provider_summarize)
DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_runner("provider.tag", _run_provider_tag)
DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_runner("provider.generate_json", _run_provider_generate_json)
DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_runner("echo", _run_echo)
DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_runner("local.task", _run_local_task)


def register_continuation_runner(runner_type: str, handler: RunnerHandler) -> None:
    DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_runner(runner_type, handler)


def register_continuation_provider_kind(kind: str, resolver: ProviderResolver) -> None:
    DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_provider_kind(kind, resolver)
