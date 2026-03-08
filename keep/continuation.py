"""Local continuation runtime wiring.

Binds the shared continuation engine to local SQLite storage and local
execution adapters.
"""

from __future__ import annotations

from pathlib import Path

from .continuation_engine import (
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
    ContinuationEngine,
)
from .continuation_env import ContinuationRuntimeEnv
from .continuation_executor import LocalWorkExecutor
from .continuation_store import SQLiteFlowStore


class LocalContinuationRuntime(ContinuationEngine):
    """Local runtime composition for continuation engine."""

    def __init__(self, db_path: Path, env: ContinuationRuntimeEnv) -> None:
        super().__init__(
            flow_store=SQLiteFlowStore(db_path),
            env=env,
            work_executor=LocalWorkExecutor(env),
        )
