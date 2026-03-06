# langchain-keep

LangChain integration for [keep](https://github.com/keepnotes-ai/keep) — reflective memory for AI agents.

This is a convenience package that installs `keep-skill[langchain]` and re-exports the integration components.

## Installation

```bash
pip install langchain-keep
```

## Usage

```python
from langchain_keep import KeepStore, KeepNotesToolkit, KeepNotesRetriever

# LangGraph BaseStore
store = KeepStore()

# LangChain tools
from keep import Keeper
toolkit = KeepNotesToolkit(keeper=Keeper())
tools = toolkit.get_tools()

# RAG retriever
retriever = KeepNotesRetriever(keeper=Keeper(), k=5)
```

## What's included

| Component | Description |
|-----------|-------------|
| `KeepStore` | LangGraph `BaseStore` backed by Keep |
| `KeepNotesToolkit` | 4 LangChain tools (remember, recall, get/set context) |
| `KeepNotesRetriever` | `BaseRetriever` with optional now-context |
| `KeepNotesMiddleware` | LCEL runnable for auto-injecting memory context |

## Configuration

You need an embedding provider configured. Simplest:

```bash
export OPENAI_API_KEY=...    # or GEMINI_API_KEY
```

Or use the hosted service:

```bash
export KEEPNOTES_API_KEY=... # Sign up at https://keepnotes.ai
```

See the [full documentation](https://docs.keepnotes.ai) for all provider options.

## Links

- [Documentation](https://docs.keepnotes.ai)
- [GitHub](https://github.com/keepnotes-ai/keep)
- [keep on PyPI](https://pypi.org/project/keep-skill/)
