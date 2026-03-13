# keep config

Show configuration and resolve paths.

## Usage

```bash
keep config                           # Show all config
keep config file                      # Config file location
keep config tool                      # Package directory (SKILL.md location)
keep config docs                      # Documentation directory
keep config store                     # Store path
keep config openclaw-plugin           # OpenClaw plugin directory
keep config providers                 # All provider config
keep config providers.embedding       # Embedding provider name
```

## Options

| Option | Description |
|--------|-------------|
| `--reset-system-docs` | Force reload system documents from bundled content |
| `-s`, `--store PATH` | Override store directory |

## Config file location

The config file is `keep.toml` inside the config directory. The config directory is resolved in this order:

1. **`KEEP_CONFIG` environment variable** — explicit path to config directory
2. **Tree-walk** — search from current directory up to `~` for `.keep/keep.toml`
3. **Default** — `~/.keep/`

The tree-walk enables project-local stores: place a `.keep/keep.toml` in your project root and `keep` will use it when you're in that directory tree.

## Store path resolution

The store (where data lives) is resolved separately from config:

1. **`--store` CLI option** — per-command override
2. **`KEEP_STORE_PATH` environment variable**
3. **`store.path` in config file** — `[store]` section of `keep.toml`
4. **Config directory itself** — backwards compatibility default

## Config file format

```toml
[store]
version = 2
max_summary_length = 1000

[embedding]
name = "ollama"                        # or "voyage", "openai", "gemini", "mistral", "mlx", "sentence-transformers"
model = "nomic-embed-text"

[summarization]
name = "ollama"                        # or "anthropic", "openai", "gemini", "mistral", "mlx"
model = "gemma3:1b"

[media]
name = "ollama"                        # or "mlx" (auto-detected)
# vision_model = "llava"              # Ollama vision model for image description

[document]
name = "composite"

[tags]
project = "my-project"                 # Default tags applied to all new items
owner = "alice"
required = ["user"]                    # Tags that must be present on every put()
namespace_keys = ["category", "user"]  # LangGraph namespace-to-tag mapping
```

### Tags section details

- **Default tags** (key = "value") — Applied to all new items. Overridden by user tags.
- **`required`** — List of tag keys that must be present on every `put()` call. Raises `ValueError` if missing. System docs (dot-prefix IDs like `.meta/`) are exempt.
- **`namespace_keys`** — Positional mapping for [LangChain integration](LANGCHAIN-INTEGRATION.md). Maps LangGraph namespace tuple components to Keep tag names.

## Providers

Keep needs an embedding provider (for search) and a summarization provider (for summaries). Most providers do both.

### Hosted Service

Sign up at [keepnotes.ai](https://keepnotes.ai) to get an API key — no local models, no database setup:

```bash
export KEEPNOTES_API_KEY=kn_...
keep put "test"                    # That's it — storage, search, and summarization handled
```

Works across all your tools (Claude Code, Kiro, Codex) with the same API key. Project isolation, media pipelines, and backups are managed for you.

### Ollama (Recommended Local Option)

[Ollama](https://ollama.com/) is the easiest way to run keep locally with no API keys. Install Ollama and go — keep handles the rest:

```bash
# 1. Install Ollama from https://ollama.com/
# 2. That's it:
keep put "test"                     # Auto-detected, models pulled automatically
```

Keep auto-detects Ollama and pulls the models it needs on first use. It picks the best available model for each task: dedicated embedding models for embeddings, generative models for summarization. Respects `OLLAMA_HOST` if set.

Ollama runs models in a separate server process, so keep itself stays lightweight (~36 MB RSS) regardless of model size.

### API Providers

Set environment variables for your preferred providers:

| Provider | Env Variable | Get API Key | Embeddings | Summarization |
|----------|--------------|-------------|------------|---------------|
| **Voyage AI** | `VOYAGE_API_KEY` | [dash.voyageai.com](https://dash.voyageai.com/) | yes | - |
| **Anthropic** | `ANTHROPIC_API_KEY` or `CLAUDE_CODE_OAUTH_TOKEN`* | [console.anthropic.com](https://console.anthropic.com/) | - | yes |
| **OpenAI** | `OPENAI_API_KEY` | [platform.openai.com](https://platform.openai.com/) | yes | yes |
| **Google Gemini** | `GEMINI_API_KEY` | [aistudio.google.com](https://aistudio.google.com/) | yes | yes |
| **Mistral** | `MISTRAL_API_KEY` | [console.mistral.ai](https://console.mistral.ai/) | yes | yes |
| **Vertex AI** | `GOOGLE_CLOUD_PROJECT` | GCP Workload Identity / ADC | yes | yes |

\* **Anthropic Authentication Methods:**
- **API Key** (`ANTHROPIC_API_KEY`): Recommended. Get from [console.anthropic.com](https://console.anthropic.com/). Format: `sk-ant-api03-...`
- **OAuth Token** (`CLAUDE_CODE_OAUTH_TOKEN`): For Claude Pro/Team subscribers. Generate via `claude setup-token`. Format: `sk-ant-oat01-...`
  - OAuth tokens from `claude setup-token` are primarily designed for Claude Code CLI authentication
  - For production use with `keep`, prefer using a standard API key from the Anthropic console

**Simplest setup** (single API key):
```bash
export OPENAI_API_KEY=...      # Does both embeddings + summarization
# Or: GEMINI_API_KEY=...       # Also does both
keep put "test"
```

**Best quality** (two API keys for optimal embeddings):
```bash
export VOYAGE_API_KEY=...      # Embeddings (Anthropic's partner)
export ANTHROPIC_API_KEY=...   # Summarization (cost-effective: claude-3-haiku)
keep put "test"
```

### Local MLX Providers (Apple Silicon)

For offline operation without Ollama on macOS Apple Silicon. Models run in-process using Metal acceleration — faster cold-start but higher memory usage (~1 GB+). Ollama is generally recommended instead for better stability and performance, especially for background processing.

```bash
uv tool install 'keep-skill[local]'
keep put "test"             # No API key needed
```

### Claude Desktop Setup

For use in Claude Desktop, API-based providers can be used. For OpenAI (handles both embeddings and summarization):

1. **Get an OpenAI API key** at [platform.openai.com](https://platform.openai.com/)
2. **Add to network allowlist**: `api.openai.com`
3. **Set `OPENAI_API_KEY`** and use normally

Alternatively, for best quality embeddings with Anthropic summarization:

1. **Get API keys** at [dash.voyageai.com](https://dash.voyageai.com/) and [console.anthropic.com](https://console.anthropic.com/)
2. **Add to network allowlist**: `api.voyageai.com`, `api.anthropic.com`
3. **Set both `VOYAGE_API_KEY` and `ANTHROPIC_API_KEY`**

### Available Models

| Provider | Type | Models |
|----------|------|--------|
| **Voyage** | Embeddings | `voyage-3.5-lite` (default), `voyage-3-large`, `voyage-code-3` |
| **Anthropic** | Summarization | `claude-3-haiku-20240307` (default, $0.25/MTok), `claude-3-5-haiku-20241022` |
| **OpenAI** | Embeddings | `text-embedding-3-small` (default), `text-embedding-3-large` |
| **OpenAI** | Summarization | `gpt-4o-mini` (default), `gpt-4o` |
| **Gemini** | Embeddings | `text-embedding-004` (default) |
| **Gemini** | Summarization | `gemini-2.5-flash` (default), `gemini-2.5-pro` |
| **Mistral** | Embeddings | `mistral-embed` (default, 1024 dims) |
| **Mistral** | Summarization | `mistral-small-latest` (default), `mistral-large-latest` |
| **Mistral** | OCR | `mistral-ocr-latest` — cloud OCR for images and PDFs |
| **Ollama** | Embeddings | `nomic-embed-text` (recommended), `mxbai-embed-large` |
| **Ollama** | Summarization | `gemma3:1b` (fast), `llama3.2:3b`, `mistral`, `phi3` |
| **Ollama** | Media | Vision models: `llava`, `moondream`, `bakllava` (images only) |
| **Ollama** | OCR | `glm-ocr` (auto-pulled on first use) — scanned PDFs and images |
| **MLX** | Embeddings | `all-MiniLM-L6-v2` (sentence-transformers, Apple Silicon only) |
| **MLX** | Summarization | MLX models, e.g. `Llama-3.2-3B-Instruct-4bit` (Apple Silicon only) |
| **MLX** | Media | `mlx-vlm` for images, `mlx-whisper` for audio (Apple Silicon only) |
| **MLX** | OCR | `mlx-vlm` vision models (Apple Silicon only) |

### Media Description (optional)

When configured, images and audio files get model-generated descriptions alongside their extracted metadata, making them semantically searchable. Without this, media files are indexed with metadata only (EXIF, ID3 tags).

```toml
[media]
name = "mlx"
vision_model = "mlx-community/Qwen2-VL-2B-Instruct-4bit"
whisper_model = "mlx-community/whisper-large-v3-turbo"
```

Install media dependencies (Apple Silicon): `pip install keep-skill[media]`

Auto-detected if `mlx-vlm` or `mlx-whisper` is installed, or if Ollama has a vision model (e.g. `llava`).

## Environment variables

```bash
KEEP_STORE_PATH=/path/to/store       # Override store location
KEEP_CONFIG=/path/to/.keep           # Override config directory
KEEP_TAG_PROJECT=myapp               # Auto-apply tags (any KEEP_TAG_* variable)
KEEP_VERBOSE=1                       # Debug logging to stderr
KEEP_NO_SETUP=1                      # Skip auto-install of tool integrations
OLLAMA_HOST=http://localhost:11434   # Ollama server URL (auto-detected)
OPENAI_API_KEY=sk-...                # For OpenAI (embeddings + summarization)
GEMINI_API_KEY=...                   # For Gemini (embeddings + summarization)
GOOGLE_CLOUD_PROJECT=my-project      # Vertex AI via Workload Identity / ADC
GOOGLE_CLOUD_LOCATION=us-east1       # Vertex AI region (default: us-east1)
VOYAGE_API_KEY=pa-...                # For Voyage embeddings only
ANTHROPIC_API_KEY=sk-ant-...         # For Anthropic summarization only
MISTRAL_API_KEY=...                  # For Mistral (embeddings + summarization + OCR)
CLAUDE_CODE_OAUTH_TOKEN=sk-ant-oat01-...  # OAuth token alternative
KEEPNOTES_API_KEY=kn_...             # For hosted keepnotes.ai service
```

## Data security

### Encryption at rest

Keep stores data in SQLite databases and ChromaDB files on disk. These are **not encrypted** by default.

If you store sensitive content (plans, credentials, reasoning traces), enable disk encryption:

| OS | Solution | How |
|----|----------|-----|
| **macOS** | FileVault | System Settings > Privacy & Security > FileVault |
| **Linux** | LUKS | Encrypt home directory or the partition containing `~/.keep/` |
| **Windows** | BitLocker | Settings > Privacy & security > Device encryption |

This is the recommended approach because it transparently covers both SQLite and ChromaDB's internal storage without application-level changes.

## Troubleshooting

**No embedding provider configured:** Set an API key (e.g., `VOYAGE_API_KEY`), install Ollama with models, or install `keep-skill[local]`.

**Model download hangs:** First use of local models downloads weights (~minutes). Cached in `~/.cache/`.

**ChromaDB errors:** Delete `~/.keep/chroma/` to reset.

**Slow local summarization:** Large content is summarized in the background automatically. Use `keep pending` to monitor progress.

**Claude Code hooks need `jq`:** The prompt-submit hook uses `jq` to extract context. Install with your package manager (e.g., `brew install jq`). Hooks are fail-safe without it, but prompt context won't be captured.

## Config subpaths

| Path | Returns |
|------|---------|
| `file` | Config file path (`~/.keep/keep.toml`) |
| `tool` | Package directory (where SKILL.md lives) |
| `docs` | Documentation directory |
| `store` | Store data path |
| `openclaw-plugin` | OpenClaw plugin directory |
| `providers` | All provider configuration |
| `providers.embedding` | Embedding provider name |
| `providers.summarization` | Summarization provider name |
| `providers.media` | Media description provider name |

Subpath output is raw (unquoted) for shell scripting:

```bash
cat "$(keep config tool)/SKILL.md"    # Read the practice guide
ls "$(keep config store)"             # List store contents
```

## Resetting system documents

System documents (`.conversations`, `.domains`, `.tag/*`, etc.) are bundled with keep and loaded on first use. If they've been modified or corrupted:

```bash
keep config --reset-system-docs       # Reload all from bundled content
```

## See Also

- [QUICKSTART.md](QUICKSTART.md) — Get started with keep in 5 minutes
- [REFERENCE.md](REFERENCE.md) — Quick reference index
- [LANGCHAIN-INTEGRATION.md](LANGCHAIN-INTEGRATION.md) — LangChain/LangGraph integration
- [ARCHITECTURE.md](ARCHITECTURE.md) — System internals
