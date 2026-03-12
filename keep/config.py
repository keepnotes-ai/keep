"""Configuration management for reflective memory stores.

The configuration is stored as a TOML file in the store directory.
It specifies which providers to use and their parameters.
"""

import importlib.resources
import json
import os
import platform
import tomllib
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# tomli_w for writing TOML (tomllib is read-only)
import tomli_w


CONFIG_FILENAME = "keep.toml"
CONFIG_VERSION = 3  # Bumped for document versioning support
SYSTEM_DOCS_VERSION = 20  # Legacy — kept for backward-compat reading of old configs


def get_tool_directory() -> Path:
    """Return keep package directory (contains SKILL.md and docs/library/).

    For installed package: the keep/ package directory itself (SKILL.md is inside).
    For development: the repository root (one level up from keep/).
    """
    keep_pkg = importlib.resources.files("keep")
    pkg_path = Path(str(keep_pkg))

    # Check if SKILL.md is in the package (installed via wheel with force-include)
    if (pkg_path / "SKILL.md").exists():
        return pkg_path

    # Development: SKILL.md is at repo root (parent of keep/)
    if (pkg_path.parent / "SKILL.md").exists():
        return pkg_path.parent

    # Fallback: return the package directory
    return pkg_path


# Providers that indicate "nothing good found" — not persisted to keep.toml.
# When load_config() sees these (or missing sections), it re-detects.
_FALLBACK_PROVIDERS = frozenset({"truncate"})


@dataclass
class ProviderConfig:
    """Configuration for a single provider."""
    name: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class EmbeddingIdentity:
    """Identity of an embedding model for compatibility checking.
    
    Two embeddings are compatible only if they have the same identity.
    Different models, even with the same dimension, produce incompatible vectors.
    """
    provider: str  # e.g., "sentence-transformers", "openai"
    model: str     # e.g., "all-MiniLM-L6-v2", "text-embedding-3-small"
    dimension: int # e.g., 384, 1536
    
    @property
    def key(self) -> str:
        """Short key for collection naming.
        
        Format: {provider}_{model_slug}
        e.g., "st_MiniLM_L6_v2", "openai_3_small"
        """
        # Simplify model name for use in collection names
        # ChromaDB collection names only allow [a-zA-Z0-9_-]
        model_slug = self.model.replace("/", "_").replace("-", "_").replace(".", "_").replace(":", "_")
        # Remove common prefixes
        for prefix in ["all_", "text_embedding_"]:
            if model_slug.lower().startswith(prefix):
                model_slug = model_slug[len(prefix):]
        # Shorten provider names
        provider_short = {
            "sentence-transformers": "st",
            "openai": "openai",
            "gemini": "gemini",
            "ollama": "ollama",
            "voyage": "voyage",
            "mistral": "mistral",
        }.get(self.provider, self.provider[:6])
        
        return f"{provider_short}_{model_slug}"


@dataclass
class RemoteConfig:
    """Configuration for remote keepnotes.ai backend."""
    api_url: str  # e.g., "https://api.keepnotes.ai"
    api_key: str  # e.g., "kn_live_..."
    project: Optional[str] = None  # project slug for X-Project header


@dataclass
class StoreConfig:
    """Complete store configuration."""
    path: Path  # Store path (where data lives)
    config_dir: Optional[Path] = None  # Where config was loaded from (may differ from path)
    store_path: Optional[str] = None  # Explicit store.path from config file (raw string)
    version: int = CONFIG_VERSION
    created: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    # Provider configurations (embedding may be None if no provider is available)
    embedding: Optional[ProviderConfig] = field(default_factory=lambda: ProviderConfig("sentence-transformers"))
    summarization: ProviderConfig = field(default_factory=lambda: ProviderConfig("truncate"))
    document: ProviderConfig = field(default_factory=lambda: ProviderConfig("composite"))

    # Media description provider (optional - if None, media indexing is metadata-only)
    media: Optional[ProviderConfig] = None

    # Analyzer provider (optional - if None, uses SlidingWindowAnalyzer wrapping summarization)
    analyzer: Optional[ProviderConfig] = None

    # Content extractor (optional - if None, scanned PDFs fall back to text-only extraction)
    content_extractor: Optional[ProviderConfig] = None

    # Embedding identity (set after first use, used for validation)
    embedding_identity: Optional[EmbeddingIdentity] = None

    # Default tags applied to all update/remember operations
    default_tags: dict[str, str] = field(default_factory=dict)

    # Maximum length for summaries (used for smart remember and validation)
    max_summary_length: int = 2000

    # Maximum length for inline content (CLI put with text or stdin).
    # Content longer than this must be stored as a file (keep put file://...).
    # Default 2000; increase for benchmarks or bulk ingestion of larger documents.
    max_inline_length: int = 2000

    # Maximum file size in bytes for document fetching (default 100MB)
    max_file_size: int = 100_000_000

    # Composite hash of bundled system docs (auto-computed, replaces manual version)
    system_docs_hash: str = ""

    # True once this store has been scanned for legacy Chroma tag metadata.
    # Avoids re-running an O(N) startup check on every process invocation.
    chroma_tag_markers_verified: bool = False

    # Tool integrations tracking (presence of key = handled, value = installed or skipped)
    integrations: dict[str, Any] = field(default_factory=dict)

    # Remote backend (if set, Keeper delegates to keepnotes.ai API)
    remote: Optional[RemoteConfig] = None

    # Required tags — put() raises ValueError if any of these keys are missing.
    # System notes (dot-prefix IDs like .meta/*, .tag/*) are exempt.
    required_tags: list[str] = field(default_factory=list)

    # Namespace keys — positional mapping from namespace components to tag names.
    # Used by KeepStore (LangGraph BaseStore) to map namespace tuples to Keep tags.
    # Example: ["category", "user"] means position 0 → category, position 1 → user.
    namespace_keys: list[str] = field(default_factory=list)

    # Pluggable backend ("local" = default, or entry-point name)
    backend: str = "local"
    backend_params: dict[str, Any] = field(default_factory=dict)

    # Default tick budget for `keep flow` invocations
    budget_per_flow: int = 5

    @property
    def config_path(self) -> Path:
        """Path to the TOML config file."""
        config_location = self.config_dir if self.config_dir else self.path
        return config_location / CONFIG_FILENAME

    def exists(self) -> bool:
        """Check if config file exists."""
        return self.config_path.exists()




def _detect_ollama() -> dict | None:
    """Check if Ollama is running locally and discover available models.

    Respects OLLAMA_HOST environment variable (default: http://localhost:11434).
    Uses a short timeout (0.5s) to avoid blocking during provider detection.

    Returns dict with 'base_url' and 'models' if Ollama is reachable
    with at least one model, None otherwise.
    """
    base_url = os.environ.get("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
    if not base_url.startswith("http"):
        base_url = f"http://{base_url}"

    try:
        from .types import user_agent
        req = urllib.request.Request(
            f"{base_url}/api/tags",
            headers={"User-Agent": user_agent()},
        )
        with urllib.request.urlopen(req, timeout=0.5) as resp:
            data = json.loads(resp.read())
            models = [m["name"] for m in data.get("models", [])]
            if models:
                return {"base_url": base_url, "models": models}
    except (OSError, ValueError):
        pass  # Ollama not running or not responding
    return None


def _ollama_has_model(model: str, base_url: str | None = None) -> bool:
    """Check if a specific model is available in Ollama."""
    ollama = _detect_ollama()
    if not ollama:
        return False
    if base_url and ollama["base_url"] != base_url:
        return False
    base = model.split(":")[0]
    return any(m.split(":")[0] == base for m in ollama["models"])


def ollama_pull(model: str, base_url: str | None = None,
                on_progress: "Callable[[str], None] | None" = None) -> bool:
    """Pull an Ollama model, streaming progress.

    Returns True if model pulled successfully, False on error.
    on_progress receives status strings like "pulling abc123... 45%".
    """
    url = (base_url or os.environ.get("OLLAMA_HOST", "http://localhost:11434")).rstrip("/")
    if not url.startswith("http"):
        url = f"http://{url}"

    try:
        data = json.dumps({"name": model, "stream": True}).encode()
        from .types import user_agent
        req = urllib.request.Request(
            f"{url}/api/pull", data=data,
            headers={"Content-Type": "application/json", "User-Agent": user_agent()},
        )
        with urllib.request.urlopen(req, timeout=600) as resp:
            buf = b""
            while True:
                chunk = resp.read(512)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if not line.strip():
                        continue
                    msg = json.loads(line)
                    if on_progress:
                        status = msg.get("status", "")
                        total = msg.get("total", 0)
                        completed = msg.get("completed", 0)
                        if total and completed:
                            pct = int(completed / total * 100)
                            on_progress(f"{status} {pct}%")
                        elif status:
                            on_progress(status)
                    if msg.get("error"):
                        return False
        return True
    except (OSError, ValueError):
        return False


def _ollama_pick_models(models: list[str]) -> tuple[str, str | None]:
    """Choose the best Ollama models for embeddings and summarization.

    Returns (embed_model, chat_model). chat_model is None if only
    embedding-specific models are available.
    """
    # Separate embedding-specific models from generative models
    embed_models = []
    generative_models = []
    for m in models:
        base = m.split(":")[0]
        if "embed" in base:
            embed_models.append(m)
        else:
            generative_models.append(m)

    # For embeddings: prefer dedicated embedding model, else first available
    embed_model = embed_models[0] if embed_models else models[0]

    # For summarization: need a generative model (embedding models can't generate text)
    chat_model = generative_models[0] if generative_models else None

    return embed_model, chat_model


# Ollama model names (base, before ':') known to support vision.
OLLAMA_VISION_KEYWORDS = ("llava", "moondream", "bakllava", "llama3.2-vision")
# gemma3 has vision at 4b+, not 1b
_GEMMA3_MIN_VISION_SIZE = 4
# Default vision model to pull when none available.
OLLAMA_DEFAULT_VISION_MODEL = "gemma3:4b"
# Default OCR model to pull when none available.
OLLAMA_DEFAULT_OCR_MODEL = "glm-ocr"


def _ollama_vision_models(models: list[str]) -> list[str]:
    """Filter Ollama models to those known to support vision."""
    result = []
    for m in models:
        base = m.split(":")[0]
        tag = m.split(":")[-1] if ":" in m else ""
        # Check keyword matches
        if any(v in base for v in OLLAMA_VISION_KEYWORDS):
            result.append(m)
        # gemma3 has vision at 4b+
        elif base == "gemma3":
            try:
                size = int("".join(c for c in tag if c.isdigit()) or "0")
                if size >= _GEMMA3_MIN_VISION_SIZE:
                    result.append(m)
            except ValueError:
                pass
    return result


def _detect_content_extractor() -> "ProviderConfig | None":
    """Auto-detect a content extractor (OCR) provider.

    Priority: Mistral (cloud OCR) > Ollama (glm-ocr) > MLX > None.
    """
    # 1. Mistral OCR (high-quality cloud OCR)
    if not os.environ.get("KEEP_LOCAL_ONLY") and os.environ.get("MISTRAL_API_KEY"):
        return ProviderConfig("mistral")

    # 2. Ollama
    ollama = _detect_ollama()
    if ollama:
        params: dict[str, Any] = {"model": "glm-ocr"}
        if ollama["base_url"] != "http://localhost:11434":
            params["base_url"] = ollama["base_url"]
        return ProviderConfig("ollama", params)

    # 3. MLX (Apple Silicon with mlx-vlm)
    if platform.system() == "Darwin" and platform.machine() == "arm64":
        try:
            import mlx_vlm  # noqa
            return ProviderConfig("mlx")
        except ImportError:
            pass

    return None


def detect_default_providers() -> dict[str, ProviderConfig | None]:
    """Detect the best default providers for the current environment.

    Priority for embeddings:
    1. API keys: VOYAGE_API_KEY, OPENAI_API_KEY, GEMINI_API_KEY
    2. Ollama (if running locally with models)
    3. Local models (if installed)
    4. None if nothing available

    Priority for summarization:
    1. API keys: ANTHROPIC_API_KEY (or CLAUDE_CODE_OAUTH_TOKEN), OPENAI_API_KEY, GEMINI_API_KEY
    2. Ollama (if running locally with a generative model)
    3. Local models (if installed)
    4. Fallback: truncate (always available)

    Returns provider configs for: embedding, summarization, document.
    embedding may be None if no provider is available.
    """
    providers: dict[str, ProviderConfig | None] = {}

    # Check for Apple Silicon with enough memory for local ML models.
    # MLX models need significant RAM; skip on machines with < 16 GB
    # to prevent OOM crashes.
    _is_arm_mac = (
        platform.system() == "Darwin" and
        platform.machine() == "arm64"
    )
    if _is_arm_mac:
        try:
            _mem_gb = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / (1024 ** 3)
        except (ValueError, OSError, AttributeError):
            _mem_gb = 0
        is_apple_silicon = _mem_gb >= 16
    else:
        is_apple_silicon = False

    # KEEP_LOCAL_ONLY=1 suppresses remote API provider auto-detection
    local_only = bool(os.environ.get("KEEP_LOCAL_ONLY"))

    # Check for API keys (skipped in local-only mode)
    has_anthropic_key = not local_only and bool(
        os.environ.get("ANTHROPIC_API_KEY") or
        os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
    )
    has_openai_key = not local_only and bool(
        os.environ.get("KEEP_OPENAI_API_KEY") or
        os.environ.get("OPENAI_API_KEY")
    )
    has_gemini_key = not local_only and bool(
        os.environ.get("GEMINI_API_KEY") or
        os.environ.get("GOOGLE_API_KEY") or
        os.environ.get("GOOGLE_CLOUD_PROJECT")
    )
    has_voyage_key = not local_only and bool(os.environ.get("VOYAGE_API_KEY"))
    has_mistral_key = not local_only and bool(os.environ.get("MISTRAL_API_KEY"))

    # Check for Ollama (lazy — only probed when no API key covers both)
    _ollama_info: dict | None = None
    _ollama_checked = False

    def get_ollama() -> dict | None:
        nonlocal _ollama_info, _ollama_checked
        if not _ollama_checked:
            _ollama_checked = True
            _ollama_info = _detect_ollama()
        return _ollama_info

    # --- Embedding provider ---
    # Priority: Voyage > OpenAI > Gemini > Ollama > MLX > sentence-transformers
    embedding_provider: ProviderConfig | None = None

    # 1. API providers first (Voyage uses direct REST, no SDK import needed)
    if has_voyage_key:
        embedding_provider = ProviderConfig("voyage", {"model": "voyage-3.5-lite"})
    elif has_openai_key:
        embedding_provider = ProviderConfig("openai")
    elif has_gemini_key:
        embedding_provider = ProviderConfig("gemini")
    elif has_mistral_key:
        embedding_provider = ProviderConfig("mistral")

    # 2. Ollama (local server, no API key needed)
    if embedding_provider is None:
        ollama = get_ollama()
        if ollama:
            embed_model, _ = _ollama_pick_models(ollama["models"])
            params: dict[str, Any] = {"model": embed_model}
            if ollama["base_url"] != "http://localhost:11434":
                params["base_url"] = ollama["base_url"]
            embedding_provider = ProviderConfig("ollama", params)

    # 3. Local providers (MLX, sentence-transformers)
    if embedding_provider is None:
        if is_apple_silicon:
            try:
                import mlx.core  # noqa
                import sentence_transformers  # noqa  — MLX embedding uses sentence-transformers
                embedding_provider = ProviderConfig("mlx", {"model": "all-MiniLM-L6-v2"})
            except ImportError:
                pass

        if embedding_provider is None:
            try:
                import sentence_transformers  # noqa
                embedding_provider = ProviderConfig("sentence-transformers")
            except ImportError:
                pass

    # May be None - CLI will show helpful error
    providers["embedding"] = embedding_provider

    # --- Summarization provider ---
    # Priority: Anthropic > OpenAI > Gemini > Ollama > MLX > truncate
    summarization_provider: ProviderConfig | None = None

    # 1. API providers
    if has_anthropic_key:
        summarization_provider = ProviderConfig("anthropic", {"model": "claude-haiku-4-5-20251001"})
    elif has_openai_key:
        summarization_provider = ProviderConfig("openai")
    elif has_gemini_key:
        summarization_provider = ProviderConfig("gemini")
    elif has_mistral_key:
        summarization_provider = ProviderConfig("mistral")

    # 2. Ollama (needs a generative model, not embedding-only)
    if summarization_provider is None:
        ollama = get_ollama()
        if ollama:
            _, chat_model = _ollama_pick_models(ollama["models"])
            if chat_model:
                params = {"model": chat_model}
                if ollama["base_url"] != "http://localhost:11434":
                    params["base_url"] = ollama["base_url"]
                summarization_provider = ProviderConfig("ollama", params)

    # 3. Local MLX (Apple Silicon)
    if summarization_provider is None and is_apple_silicon:
        try:
            import mlx_lm  # noqa
            summarization_provider = ProviderConfig("mlx", {"model": "mlx-community/Llama-3.2-3B-Instruct-4bit"})
        except ImportError:
            pass

    # 4. Fallback: truncate (always available)
    if summarization_provider is None:
        summarization_provider = ProviderConfig("truncate")

    providers["summarization"] = summarization_provider

    # --- Media description provider ---
    # Priority: Ollama (if has vision model) > MLX (Apple Silicon) > None
    media_provider: ProviderConfig | None = None

    # 1. Ollama with a vision-capable model
    if media_provider is None:
        ollama = get_ollama()
        if ollama:
            vision_models = _ollama_vision_models(ollama["models"])
            if vision_models:
                params: dict[str, Any] = {"model": vision_models[0]}
                if ollama["base_url"] != "http://localhost:11434":
                    params["base_url"] = ollama["base_url"]
                media_provider = ProviderConfig("ollama", params)

    # 2. MLX (Apple Silicon with mlx-vlm or mlx-whisper)
    if media_provider is None and is_apple_silicon:
        _has_media_mlx = False
        try:
            import mlx_vlm  # noqa
            _has_media_mlx = True
        except ImportError:
            pass
        if not _has_media_mlx:
            try:
                import mlx_whisper  # noqa
                _has_media_mlx = True
            except ImportError:
                pass
        if _has_media_mlx:
            media_provider = ProviderConfig("mlx")

    providers["media"] = media_provider

    # --- Content extractor (OCR) ---
    providers["content_extractor"] = _detect_content_extractor()

    # Document provider is always composite
    providers["document"] = ProviderConfig("composite")

    return providers


def create_default_config(config_dir: Path, store_path: Optional[Path] = None) -> StoreConfig:
    """Create a new config with minimal defaults.

    Fallback providers (truncate, None) are not persisted to keep.toml.
    On next load, load_config() re-detects if better options are available.
    Real providers (mlx, ollama, voyage, etc.) are persisted once detected.

    Args:
        config_dir: Directory where keep.toml will be saved
        store_path: Optional explicit store location (if different from config_dir)
    """
    providers = detect_default_providers()

    # If store_path is provided and different from config_dir, record it
    store_path_str = None
    actual_store = config_dir
    if store_path and store_path.resolve() != config_dir.resolve():
        store_path_str = str(store_path)
        actual_store = store_path

    return StoreConfig(
        path=actual_store,
        config_dir=config_dir,
        store_path=store_path_str,
        embedding=providers["embedding"],
        summarization=providers["summarization"],
        document=providers["document"],
        media=providers.get("media"),
        content_extractor=providers.get("content_extractor"),
    )


def load_config(config_dir: Path) -> StoreConfig:
    """Load configuration from a config directory.

    The config_dir is where keep.toml lives. The actual store location
    may be different if store.path is set in the config.

    Args:
        config_dir: Directory containing keep.toml

    Raises:
        FileNotFoundError: If config doesn't exist
        ValueError: If config is invalid
    """
    config_path = config_dir / CONFIG_FILENAME

    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    with open(config_path, "rb") as f:
        data = tomllib.load(f)

    # Validate version
    version = data.get("store", {}).get("version", 1)
    if version > CONFIG_VERSION:
        raise ValueError(f"Config version {version} is newer than supported ({CONFIG_VERSION})")

    # Parse store.path - explicit store location
    store_path_str = data.get("store", {}).get("path")
    if store_path_str:
        actual_store = Path(store_path_str).expanduser().resolve()
    else:
        actual_store = config_dir  # Backwards compat: store is at config location

    # Parse provider configs
    def parse_provider(section: dict) -> ProviderConfig:
        return ProviderConfig(
            name=section.get("name", ""),
            params={k: v for k, v in section.items() if k != "name"},
        )

    # Parse default tags, required tags, and namespace keys from [tags] section
    raw_tags = data.get("tags", {})
    required_tags = raw_tags.pop("required", [])
    if isinstance(required_tags, str):
        required_tags = [required_tags]
    namespace_keys = raw_tags.pop("namespace_keys", [])
    if isinstance(namespace_keys, str):
        namespace_keys = [namespace_keys]
    default_tags = {k: str(v) for k, v in raw_tags.items()
                    if not k.startswith("_")}

    # Parse max_summary_length (default 2000)
    max_summary_length = data.get("store", {}).get("max_summary_length", 2000)

    # Parse max_inline_length (default 2000; falls back to max_summary_length for compat)
    max_inline_length = data.get("store", {}).get(
        "max_inline_length",
        max_summary_length,  # backward compat: if not set, use max_summary_length
    )

    # Parse max_file_size (default 100MB)
    max_file_size = data.get("store", {}).get("max_file_size", 100_000_000)

    # Parse system_docs_hash (replaces legacy system_docs_version integer).
    # Backward compat: if old integer version exists but no hash, set hash to ""
    # so migration runs and computes the real hash.
    system_docs_hash = data.get("store", {}).get("system_docs_hash", "")
    chroma_tag_markers_verified = bool(
        data.get("store", {}).get("chroma_tag_markers_verified", False)
    )

    # Parse integrations section (presence = handled)
    integrations = data.get("integrations", {})

    # Parse optional media section
    media_config = parse_provider(data["media"]) if "media" in data else None

    # Parse optional analyzer section
    analyzer_config = parse_provider(data["analyzer"]) if "analyzer" in data else None

    # Parse optional content_extractor section
    content_extractor_config = parse_provider(data["content_extractor"]) if "content_extractor" in data else None

    # Parse remote backend config (env vars override TOML)
    remote = None
    remote_data = data.get("remote", {})
    api_url = os.environ.get("KEEPNOTES_API_URL") or remote_data.get("api_url", "https://api.keepnotes.ai")
    api_key = os.environ.get("KEEPNOTES_API_KEY") or remote_data.get("api_key")
    project = os.environ.get("KEEPNOTES_PROJECT") or remote_data.get("project")
    if api_url and api_key:
        remote = RemoteConfig(api_url=api_url, api_key=api_key, project=project or None)

    # Parse pluggable backend config
    backend = data.get("store", {}).get("backend", "local")
    backend_params = data.get("store", {}).get("backend_params", {})
    if backend_params and not isinstance(backend_params, dict):
        raise ValueError("store.backend_params must be a table/dict")

    embedding_config = parse_provider(data["embedding"]) if "embedding" in data else None
    summarization_config = parse_provider(data.get("summarization", {"name": "truncate"}))

    # Auto-heal: re-detect if key providers are missing or fallback.
    # This recovers from first-run in constrained environments and
    # auto-configures media/content_extractor when available.
    _need_redetect = (
        "embedding" not in data or
        "summarization" not in data or
        summarization_config.name in _FALLBACK_PROVIDERS or
        media_config is None or
        content_extractor_config is None
    )
    if _need_redetect:
        try:
            detected = detect_default_providers()
            if "embedding" not in data and detected["embedding"] is not None:
                embedding_config = detected["embedding"]
            if "summarization" not in data or summarization_config.name in _FALLBACK_PROVIDERS:
                det_summ = detected["summarization"]
                if det_summ and det_summ.name not in _FALLBACK_PROVIDERS:
                    summarization_config = det_summ
            if media_config is None:
                media_config = detected.get("media")
            if content_extractor_config is None:
                content_extractor_config = detected.get("content_extractor")
        except Exception:
            pass  # re-detection is best-effort; don't block config loading

    # KEEP_LOCAL_ONLY=1 overrides remote providers to None/fallback
    if os.environ.get("KEEP_LOCAL_ONLY"):
        _REMOTE_PROVIDERS = {"voyage", "openai", "gemini", "anthropic", "mistral"}
        if embedding_config and embedding_config.name in _REMOTE_PROVIDERS:
            embedding_config = None
        if summarization_config and summarization_config.name in _REMOTE_PROVIDERS:
            summarization_config = ProviderConfig("truncate")
        remote = None

    budget_per_flow = int(data.get("store", {}).get("budget_per_flow", 5))

    return StoreConfig(
        path=actual_store,
        config_dir=config_dir,
        store_path=store_path_str,
        version=version,
        created=data.get("store", {}).get("created", ""),
        embedding=embedding_config,
        summarization=summarization_config,
        document=parse_provider(data.get("document", {"name": "composite"})),
        media=media_config,
        analyzer=analyzer_config,
        content_extractor=content_extractor_config,
        embedding_identity=parse_embedding_identity(data.get("embedding_identity")),
        default_tags=default_tags,
        required_tags=required_tags,
        namespace_keys=namespace_keys,
        max_summary_length=max_summary_length,
        max_inline_length=max_inline_length,
        max_file_size=max_file_size,
        system_docs_hash=system_docs_hash,
        chroma_tag_markers_verified=chroma_tag_markers_verified,
        integrations=integrations,
        remote=remote,
        backend=backend,
        backend_params=backend_params,
        budget_per_flow=budget_per_flow,
    )


def parse_embedding_identity(data: dict | None) -> EmbeddingIdentity | None:
    """Parse embedding identity from config data."""
    if data is None:
        return None
    provider = data.get("provider")
    model = data.get("model")
    dimension = data.get("dimension")
    if provider and model and dimension:
        return EmbeddingIdentity(provider=provider, model=model, dimension=dimension)
    return None


def save_config(config: StoreConfig) -> None:
    """Save configuration to the config directory.

    Creates the directory if it doesn't exist.
    """
    # Ensure config directory exists
    config_location = config.config_dir if config.config_dir else config.path
    config_location.mkdir(parents=True, exist_ok=True)

    # Build TOML structure
    def provider_to_dict(p: ProviderConfig) -> dict:
        d = {"name": p.name}
        d.update(p.params)
        return d

    store_section: dict[str, Any] = {
        "version": config.version,
        "created": config.created,
    }
    # Only write store.path if explicitly set (not default)
    if config.store_path:
        store_section["path"] = config.store_path
    # Only write max_summary_length if not default
    if config.max_summary_length != 2000:
        store_section["max_summary_length"] = config.max_summary_length
    # Only write max_inline_length if not default (and differs from max_summary_length)
    if config.max_inline_length != config.max_summary_length:
        store_section["max_inline_length"] = config.max_inline_length
    # Only write max_file_size if not default
    if config.max_file_size != 100_000_000:
        store_section["max_file_size"] = config.max_file_size
    # Write system_docs_hash if set (tracks migration state)
    if config.system_docs_hash:
        store_section["system_docs_hash"] = config.system_docs_hash
    if config.chroma_tag_markers_verified:
        store_section["chroma_tag_markers_verified"] = True
    # Only write backend if not default
    if config.backend != "local":
        store_section["backend"] = config.backend
    if config.backend_params:
        store_section["backend_params"] = config.backend_params
    if config.budget_per_flow != 5:
        store_section["budget_per_flow"] = config.budget_per_flow

    data: dict[str, Any] = {
        "store": store_section,
    }

    # Persist real providers; skip fallbacks so load_config() re-detects
    if config.embedding:
        data["embedding"] = provider_to_dict(config.embedding)
    if config.summarization and config.summarization.name not in _FALLBACK_PROVIDERS:
        data["summarization"] = provider_to_dict(config.summarization)
    if config.document:
        data["document"] = provider_to_dict(config.document)
    if config.media:
        data["media"] = provider_to_dict(config.media)
    if config.analyzer:
        data["analyzer"] = provider_to_dict(config.analyzer)
    if config.content_extractor:
        data["content_extractor"] = provider_to_dict(config.content_extractor)

    # Add embedding identity if set
    if config.embedding_identity:
        data["embedding_identity"] = {
            "provider": config.embedding_identity.provider,
            "model": config.embedding_identity.model,
            "dimension": config.embedding_identity.dimension,
        }

    # Add tags section (default tags + required + namespace_keys)
    if config.default_tags or config.required_tags or config.namespace_keys:
        tags_section = dict(config.default_tags)
        if config.required_tags:
            tags_section["required"] = config.required_tags
        if config.namespace_keys:
            tags_section["namespace_keys"] = config.namespace_keys
        data["tags"] = tags_section

    # Add integrations tracking if set
    if config.integrations:
        data["integrations"] = config.integrations

    # Add remote backend config if set (only from TOML, not env vars)
    if config.remote and not (
        os.environ.get("KEEPNOTES_API_URL") or os.environ.get("KEEPNOTES_API_KEY")
    ):
        remote_data = {
            "api_url": config.remote.api_url,
            "api_key": config.remote.api_key,
        }
        if config.remote.project:
            remote_data["project"] = config.remote.project
        data["remote"] = remote_data

    has_secrets = bool(config.remote or config.backend_params)
    if has_secrets:
        # Atomic creation with restricted permissions to prevent race condition
        fd = os.open(
            str(config.config_path),
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            0o600,
        )
        with os.fdopen(fd, "wb") as f:
            tomli_w.dump(data, f)
    else:
        with open(config.config_path, "wb") as f:
            tomli_w.dump(data, f)


def load_or_create_config(config_dir: Path, store_path: Optional[Path] = None) -> StoreConfig:
    """Load existing config or create a new one with defaults.

    This is the main entry point for config management.

    Args:
        config_dir: Directory containing (or to contain) keep.toml
        store_path: Optional explicit store location (for new configs only)
    """
    config_path = config_dir / CONFIG_FILENAME

    if config_path.exists():
        return load_config(config_dir)
    else:
        config = create_default_config(config_dir, store_path)
        save_config(config)
        return config
