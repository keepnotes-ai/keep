"""Local flow runtime wiring.

Binds the shared flow engine to local SQLite storage and local
execution adapters.
"""

from __future__ import annotations

from pathlib import Path

from .flow_engine import (
    ALLOWED_FRAME_OPS,
    BUILTIN_QUERY_AUTO_PROFILES,
    DECISION_STRATEGIES,
    DECISION_SUPPORT_VERSION,
    DEFAULT_DECISION_POLICY,
    MAX_CONTINUE_EVENTS_PER_FLOW,
    MAX_CONTINUE_PAYLOAD_BYTES,
    MAX_CONTINUE_STATE_BYTES,
    MAX_CONTINUE_WORK_INPUT_BYTES,
    MAX_CONTINUE_WORK_RESULT_BYTES,
    SYSTEM_NOTE_PREFIX,
    FlowEngine,
)
from .flow_env import FlowRuntimeEnv
from .flow_executor import LocalWorkExecutor
from .work_store import SQLiteFlowStore


class LocalFlowRuntime(FlowEngine):
    """Local runtime composition for flow engine."""

    def __init__(self, db_path: Path, env: FlowRuntimeEnv) -> None:
        super().__init__(
            flow_store=SQLiteFlowStore(db_path),
            env=env,
            work_executor=LocalWorkExecutor(env),
        )
