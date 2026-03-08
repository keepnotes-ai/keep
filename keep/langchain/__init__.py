"""LangChain integration for Keep — reflective memory for AI agents.

Components:
    KeepStore           LangGraph BaseStore backed by Keeper
    KeepNotesToolkit    4 curated tools (remember, recall, get_context, update_context)
    KeepNotesRetriever  BaseRetriever with optional now-context
    KeepNotesMiddleware LCEL runnable that auto-injects memory context

Requires: pip install keep-skill[langchain]

KeepStore also requires: pip install langgraph
"""

# Each module guards its own optional dependency (ImportError on missing packages)
from keep.langchain.middleware import KeepNotesMiddleware
from keep.langchain.retriever import KeepNotesRetriever
from keep.langchain.store import KeepStore
from keep.langchain.toolkit import KeepNotesToolkit

__all__ = [
    "KeepStore",
    "KeepNotesToolkit",
    "KeepNotesRetriever",
    "KeepNotesMiddleware",
]
