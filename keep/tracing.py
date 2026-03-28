"""OpenTelemetry tracing for keep.

Uses the otel API for instrumentation — no-ops when the SDK isn't
configured, zero overhead.  Activation:

  KEEP_TRACE=1          Enable SDK + TimingTreeProcessor (ops log output)
  OTEL_TRACES_EXPORTER  Standard otel export (Jaeger, Grafana, etc.)

Both can coexist.  Without either, all spans are no-ops.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any

from opentelemetry import trace

logger = logging.getLogger(__name__)

_tracers: dict[str, trace.Tracer] = {}


def get_tracer(name: str) -> trace.Tracer:
    """Get or create a named tracer.  Returns a no-op if SDK not initialized."""
    t = _tracers.get(name)
    if t is None:
        t = trace.get_tracer(f"keep.{name}")
        _tracers[name] = t
    return t


def init_tracing(*, tree_log: bool = False) -> None:
    """Initialize the OTel SDK if tracing is requested.

    Called once at daemon startup.  Does nothing if neither KEEP_TRACE
    nor OTEL_TRACES_EXPORTER is set.
    """
    if not (os.environ.get("KEEP_TRACE") or os.environ.get("OTEL_TRACES_EXPORTER")):
        return

    try:
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.resources import Resource
    except ImportError:
        logger.warning("opentelemetry-sdk not installed — tracing disabled")
        return

    provider = TracerProvider(resource=Resource.create({"service.name": "keep"}))

    if tree_log or os.environ.get("KEEP_TRACE"):
        provider.add_span_processor(TimingTreeProcessor())

    if os.environ.get("OTEL_TRACES_EXPORTER"):
        try:
            from opentelemetry.sdk.trace.export import BatchSpanProcessor
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                OTLPSpanExporter,
            )
            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        except ImportError:
            logger.warning("OTLP exporter not available")

    trace.set_tracer_provider(provider)
    # Clear cached tracers so they pick up the new provider
    _tracers.clear()
    logger.info("Tracing enabled")


# ---------------------------------------------------------------------------
# TimingTreeProcessor — lightweight ops-log output
# ---------------------------------------------------------------------------

class TimingTreeProcessor:
    """Collects spans per trace, prints a timing tree when the root span ends.

    Implements the SpanProcessor protocol.  Spans are buffered per trace_id
    until the root span (no parent) completes, then the full tree is logged.
    """

    def __init__(self) -> None:
        self._traces: dict[int, list[Any]] = {}
        self._lock = threading.Lock()

    def on_start(self, span: Any, parent_context: Any = None) -> None:
        pass

    def on_end(self, span: Any) -> None:
        trace_id = span.context.trace_id
        with self._lock:
            self._traces.setdefault(trace_id, []).append(span)

        # Root span completed — flush the whole trace
        if span.parent is None or not span.parent.is_valid:
            self._flush_trace(trace_id)

    def shutdown(self) -> None:
        pass

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True

    def _flush_trace(self, trace_id: int) -> None:
        with self._lock:
            spans = self._traces.pop(trace_id, [])
        if not spans:
            return

        # Separate roots from children
        roots = []
        children: dict[int, list[Any]] = {}
        for s in spans:
            if s.parent is None or not s.parent.is_valid:
                roots.append(s)
            else:
                children.setdefault(s.parent.span_id, []).append(s)

        lines: list[str] = []

        def _render(span: Any, depth: int = 0) -> None:
            dur_ms = (span.end_time - span.start_time) / 1e6
            # Inline select attributes
            attrs = ""
            if span.attributes:
                parts = []
                for k in ("cache", "count", "item_id", "query", "similar_to",
                          "http.method", "http.path"):
                    v = span.attributes.get(k)
                    if v is not None and v != "":
                        parts.append(f"{k}={v}")
                if parts:
                    attrs = f"  [{', '.join(parts)}]"
            lines.append(f"{'  ' * depth}{span.name}: {dur_ms:.1f}ms{attrs}")
            for child in sorted(
                children.get(span.context.span_id, []),
                key=lambda s: s.start_time,
            ):
                _render(child, depth + 1)

        for root in sorted(roots, key=lambda s: s.start_time):
            _render(root)

        if len(lines) > 1:
            logger.info("Trace:\n%s", "\n".join(lines))
