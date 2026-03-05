"""
Continuation work executor registry and local default executor.

This module isolates runner/provider execution from continuation state control.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from .processors import process_summarize

if TYPE_CHECKING:
    from .api import Keeper


@dataclass
class RunnerExecution:
    outputs: dict[str, Any]
    executor_id: str
    quality: dict[str, Any]


ProviderResolver = Callable[["LocalWorkExecutor", str | None, dict[str, Any] | None], tuple[Any, str]]
RunnerHandler = Callable[["LocalWorkExecutor", dict[str, Any], dict[str, Any]], RunnerExecution]


class ContinuationExecutorRegistry:
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
        keeper: "Keeper",
        *,
        registry: ContinuationExecutorRegistry | None = None,
    ) -> None:
        self._keeper = keeper
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
        handler = self._registry.get_runner(runner_type)
        return handler(self, payload, runner)


def _resolve_summarization_provider(
    executor: LocalWorkExecutor, name: str | None, params: dict[str, Any] | None,
) -> tuple[Any, str]:
    if name is None and params is None:
        return executor._keeper._get_summarization_provider(), "summarization.default"
    if name is None:
        raise ValueError("runner.provider.name is required when provider params are provided")
    from .providers.base import get_registry

    registry = get_registry()
    return registry.create_summarization(str(name), params), str(name)


def _resolve_tagging_provider(
    _executor: LocalWorkExecutor, name: str | None, params: dict[str, Any] | None,
) -> tuple[Any, str]:
    from .providers.base import get_registry

    registry = get_registry()
    if name is None:
        name = "noop"
    return registry.create_tagging(str(name), params), str(name)


def _resolve_analyzer_provider(
    _executor: LocalWorkExecutor, name: str | None, params: dict[str, Any] | None,
) -> tuple[Any, str]:
    if name is None:
        raise ValueError("runner.provider.name is required for analyzer provider")
    from .providers.base import get_registry

    registry = get_registry()
    return registry.create_analyzer(str(name), params), str(name)


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


DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_provider_kind("summarization", _resolve_summarization_provider)
DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_provider_kind("tagging", _resolve_tagging_provider)
DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_provider_kind("analyzer", _resolve_analyzer_provider)

DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_runner("provider.summarize", _run_provider_summarize)
DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_runner("provider.tag", _run_provider_tag)
DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_runner("provider.generate_json", _run_provider_generate_json)
DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_runner("echo", _run_echo)


def register_continuation_runner(runner_type: str, handler: RunnerHandler) -> None:
    DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_runner(runner_type, handler)


def register_continuation_provider_kind(kind: str, resolver: ProviderResolver) -> None:
    DEFAULT_CONTINUATION_EXECUTOR_REGISTRY.register_provider_kind(kind, resolver)
