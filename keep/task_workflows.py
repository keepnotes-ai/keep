"""Local task workflow dispatch.

Each action that supports background processing implements a ``run_task``
method.  This module provides the shared ``TaskRequest`` / ``TaskRunResult``
types and the ``run_local_task`` dispatcher that routes to the right action
via the action registry.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .api import Keeper


@dataclass
class TaskRequest:
    """Minimal task request shape shared by queue and flow paths."""

    task_type: str
    id: str
    collection: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskRunResult:
    """Outcome of a local task workflow."""

    status: str  # "applied" | "skipped"
    details: dict[str, Any] = field(default_factory=dict)


def run_local_task(keeper: "Keeper", req: TaskRequest) -> TaskRunResult:
    """Run one local task workflow by looking up the action in the registry."""
    from .actions import get_action

    task_type = str(req.task_type or "").strip()
    action = get_action(task_type)
    run_task = getattr(action, "run_task", None)
    if not callable(run_task):
        raise ValueError(
            f"action {task_type!r} does not support background tasks (no run_task method)"
        )
    return run_task(keeper, req)
