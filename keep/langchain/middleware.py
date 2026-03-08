"""KeepNotesMiddleware — auto-inject memory context into every LLM call.

Prepends ``now`` context and relevant search results to the message
list as a system message. Uses LCEL RunnableLambda (langchain-core only,
no langchain dependency).

Usage::

    from keep.langchain import KeepNotesMiddleware
    from langchain_openai import ChatOpenAI

    middleware = KeepNotesMiddleware(user_id="alice")

    # As LCEL chain — prepend context before any model
    chain = middleware.as_runnable() | ChatOpenAI(model="gpt-4o")
    response = chain.invoke([HumanMessage(content="What's my schedule?")])

    # Or apply directly to a message list
    enriched = middleware.inject(messages)
"""

from __future__ import annotations

import logging
from typing import Any, Optional, Sequence

try:
    from langchain_core.messages import BaseMessage, SystemMessage
    from langchain_core.runnables import RunnableLambda
except ImportError as e:
    raise ImportError(
        "langchain-core is required for KeepNotesMiddleware. "
        "Install with: pip install keep-skill[langchain]"
    ) from e

from keep.api import Keeper

logger = logging.getLogger(__name__)


class KeepNotesMiddleware:
    """Auto-inject Keep memory context before every LLM call.

    On each invocation:

    1. Reads ``now`` context (current state, goals, intentions)
    2. Searches memory using the last human message as query
    3. Formats results as a system message
    4. Prepends to the message list

    Args:
        keeper: An existing Keeper instance. If None, one is created.
        store: Path to the Keep store directory.
        user_id: Optional user scope for multi-user isolation.
        search_limit: Max search results to include.
        include_now: Whether to include ``now`` context.
        fail_open: If True (default), exceptions are logged and the
            original messages are returned unchanged. Memory enhances;
            it never blocks.
    """

    def __init__(
        self,
        keeper: Keeper | None = None,
        store: str | None = None,
        user_id: str | None = None,
        search_limit: int = 5,
        include_now: bool = True,
        fail_open: bool = True,
    ):
        if keeper is None:
            keeper = Keeper(store_path=store)
            keeper._get_embedding_provider()
        self._keeper = keeper
        self._user_id = user_id
        self._search_limit = search_limit
        self._include_now = include_now
        self._fail_open = fail_open

    @property
    def keeper(self) -> Keeper:
        """The underlying Keeper instance."""
        return self._keeper

    def inject(
        self,
        messages: Sequence[BaseMessage],
    ) -> list[BaseMessage]:
        """Inject memory context into a message list.

        Returns a new list with a system message prepended containing
        the ``now`` context and relevant search results.
        """
        try:
            return self._inject(messages)
        except Exception:
            if self._fail_open:
                logger.warning(
                    "KeepNotesMiddleware: injection failed, passing through",
                    exc_info=True,
                )
                return list(messages)
            raise

    def _inject(
        self,
        messages: Sequence[BaseMessage],
    ) -> list[BaseMessage]:
        parts: list[str] = []

        # 1. Now context
        if self._include_now:
            now_item = self._keeper.get_now(scope=self._user_id)
            if now_item.summary:
                parts.append(f"Current state:\n{now_item.summary}")

        # 2. Search using last human message
        query = _extract_query(messages)
        if query:
            tags = {"user": self._user_id} if self._user_id else None
            items = self._keeper.find(
                query, tags=tags, limit=self._search_limit
            )
            if items:
                lines = []
                for item in items:
                    lines.append(f"- {item.summary}")
                parts.append("Relevant notes:\n" + "\n".join(lines))

        if not parts:
            return list(messages)

        context_text = "[Memory Context]\n\n" + "\n\n".join(parts)
        context_msg = SystemMessage(content=context_text)

        return [context_msg, *messages]

    def as_runnable(self) -> RunnableLambda:
        """Return an LCEL RunnableLambda for chain composition.

        Usage::

            chain = middleware.as_runnable() | llm
        """
        return RunnableLambda(self.inject)


def _extract_query(messages: Sequence[BaseMessage]) -> str | None:
    """Extract the search query from the last human message."""
    for msg in reversed(messages):
        if msg.type == "human" and isinstance(msg.content, str):
            return msg.content
    return None
