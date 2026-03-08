"""KeepNotesToolkit — 4 curated tools for LangChain agents.

Tools:
    remember        Store a fact, preference, or decision for later recall
    recall          Search memory for relevant past notes
    get_context     Read current working context and intentions
    update_context  Update current working context

Usage::

    from keep.langchain import KeepNotesToolkit

    toolkit = KeepNotesToolkit(user_id="alice")
    tools = toolkit.get_tools()
    # [remember, recall, get_context, update_context]

    # Use with any LangChain agent
    agent = create_react_agent(llm, tools)
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

try:
    from langchain_core.tools import BaseToolkit, BaseTool, StructuredTool
except ImportError as e:
    raise ImportError(
        "langchain-core is required for KeepNotesToolkit. "
        "Install with: pip install keep-skill[langchain]"
    ) from e

from keep.api import Keeper


# ── Tool input schemas ───────────────────────────────────────────────

class RememberInput(BaseModel):
    """Input for the remember tool."""

    content: str = Field(
        description="The fact, preference, decision, or note to remember."
    )
    tags: Optional[dict[str, str]] = Field(
        default=None,
        description=(
            "Optional tags to categorize the memory. "
            "Example: {\"topic\": \"preferences\", \"project\": \"myapp\"}"
        ),
    )


class RecallInput(BaseModel):
    """Input for the recall tool."""

    query: str = Field(
        description="What to search for in memory. Use natural language."
    )
    limit: int = Field(
        default=5,
        description="Maximum number of results to return.",
    )


class UpdateContextInput(BaseModel):
    """Input for the update_context tool."""

    content: str = Field(
        description=(
            "Updated working context. Describe current state, "
            "active goals, and recent decisions."
        ),
    )


# ── Toolkit ──────────────────────────────────────────────────────────

class KeepNotesToolkit(BaseToolkit):
    """Toolkit providing 4 memory tools backed by Keep.

    Args:
        keeper: An existing Keeper instance to wrap. If None, a new one
            is created from ``store`` path.
        store: Path to the Keep store directory (e.g. ``"~/.keep"``).
            Ignored if ``keeper`` is provided.
        user_id: Optional user scope. When set, all operations are
            scoped to this user via tags and ``now:{user_id}`` context.
    """

    model_config = {"arbitrary_types_allowed": True}

    keeper: Keeper
    user_id: Optional[str] = None

    def __init__(
        self,
        keeper: Keeper | None = None,
        store: str | None = None,
        user_id: str | None = None,
        **kwargs,
    ):
        if keeper is None:
            keeper = Keeper(store_path=store)
            keeper._get_embedding_provider()
        super().__init__(keeper=keeper, user_id=user_id, **kwargs)

    def get_tools(self) -> list[BaseTool]:
        """Return the 4 memory tools."""
        return [
            self._make_remember(),
            self._make_recall(),
            self._make_get_context(),
            self._make_update_context(),
        ]

    # ── Tool factories ───────────────────────────────────────────────

    def _make_remember(self) -> BaseTool:
        keeper = self.keeper
        user_id = self.user_id

        def remember(content: str, tags: dict[str, str] | None = None) -> str:
            """Store a fact, preference, or decision for later recall."""
            merged = dict(tags or {})
            if user_id:
                merged.setdefault("user", user_id)
            item = keeper.put(content, tags=merged or None)
            return f"Remembered: {item.id}"

        return StructuredTool.from_function(
            func=remember,
            name="remember",
            description=(
                "Store a fact, preference, decision, or important note "
                "in long-term memory for later recall. Use this when the "
                "user shares something worth remembering."
            ),
            args_schema=RememberInput,
        )

    def _make_recall(self) -> BaseTool:
        keeper = self.keeper
        user_id = self.user_id

        def recall(query: str, limit: int = 5) -> str:
            """Search memory for relevant past notes."""
            tags = {"user": user_id} if user_id else None
            items = keeper.find(query, tags=tags, limit=limit)
            if not items:
                return "No relevant memories found."
            parts = []
            for item in items:
                score = f" [{item.score:.2f}]" if item.score is not None else ""
                parts.append(f"- {item.id}{score}: {item.summary}")
            return "\n".join(parts)

        return StructuredTool.from_function(
            func=recall,
            name="recall",
            description=(
                "Search long-term memory for relevant past notes, facts, "
                "preferences, or decisions. Use natural language queries."
            ),
            args_schema=RecallInput,
        )

    def _make_get_context(self) -> BaseTool:
        keeper = self.keeper
        user_id = self.user_id

        def get_context() -> str:
            """Read current working context and intentions."""
            item = keeper.get_now(scope=user_id)
            return item.summary or "(No context set)"

        return StructuredTool.from_function(
            func=get_context,
            name="get_context",
            description=(
                "Read the current working context — active goals, "
                "recent decisions, and state. Check this at the start "
                "of a session or when you need to orient."
            ),
        )

    def _make_update_context(self) -> BaseTool:
        keeper = self.keeper
        user_id = self.user_id

        def update_context(content: str) -> str:
            """Update current working context."""
            tags = {"user": user_id} if user_id else None
            item = keeper.set_now(content, scope=user_id, tags=tags)
            return f"Context updated: {item.id}"

        return StructuredTool.from_function(
            func=update_context,
            name="update_context",
            description=(
                "Update the current working context with new state, "
                "goals, or decisions. This persists across sessions."
            ),
            args_schema=UpdateContextInput,
        )
