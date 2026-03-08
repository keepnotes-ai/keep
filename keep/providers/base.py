"""Base provider protocols.

These define the interfaces that concrete providers must implement.
Using Protocol for structural subtyping - no explicit inheritance required.
"""

import json
import re
from dataclasses import dataclass
from collections.abc import Iterable
from typing import Any, Protocol, runtime_checkable


# -----------------------------------------------------------------------------
# Document Fetching
# -----------------------------------------------------------------------------

@dataclass
class Document:
    """A fetched document ready for processing.
    
    Attributes:
        uri: Original URI that was fetched
        content: Text content of the document
        content_type: MIME type if known (e.g., "text/markdown", "text/plain")
        metadata: Additional metadata from the source (headers, file stats, etc.)
        tags: Auto-extracted tags from document properties (merged with user tags)
    """
    uri: str
    content: str
    content_type: str | None = None
    metadata: dict[str, Any] | None = None
    tags: dict[str, str] | None = None


@runtime_checkable
class DocumentProvider(Protocol):
    """Fetches document content from a URI.
    
    Implementations handle specific URI schemes (file://, https://, s3://, etc.)
    and convert the content to text.
    
    Example implementation:
        class FileDocumentProvider:
            def supports(self, uri: str) -> bool:
                return uri.startswith("file://")
            
            def fetch(self, uri: str) -> Document:
                path = uri.removeprefix("file://")
                content = Path(path).read_text()
                return Document(uri=uri, content=content, content_type="text/plain")
    """
    
    def supports(self, uri: str) -> bool:
        """Check if this provider can handle the given URI.
        
        Args:
            uri: The URI to check
            
        Returns:
            True if this provider can fetch the URI
        """
        ...
    
    def fetch(self, uri: str) -> Document:
        """Fetch and return the document content.
        
        Args:
            uri: The URI to fetch
            
        Returns:
            Document with text content
            
        Raises:
            ValueError: If URI is malformed
            IOError: If document cannot be fetched
        """
        ...


# -----------------------------------------------------------------------------
# Embedding Generation
# -----------------------------------------------------------------------------

@runtime_checkable
class EmbeddingProvider(Protocol):
    """Generates vector embeddings from text.
    
    Embeddings enable semantic similarity search. The same provider instance
    must be used for both indexing and querying to ensure consistent vectors.
    
    Example implementation:
        class SentenceTransformerEmbedding:
            def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
                from sentence_transformers import SentenceTransformer
                self.model = SentenceTransformer(model_name)
            
            @property
            def dimension(self) -> int:
                return self.model.get_sentence_embedding_dimension()
            
            def embed(self, text: str) -> list[float]:
                return self.model.encode(text).tolist()
            
            def embed_batch(self, texts: list[str]) -> list[list[float]]:
                return self.model.encode(texts).tolist()
    """
    
    @property
    def dimension(self) -> int:
        """The dimensionality of the embedding vectors.
        
        This must be consistent across all calls. ChromaDb and other vector
        stores need this to configure the index.
        """
        ...
    
    def embed(self, text: str) -> list[float]:
        """Generate an embedding vector for the given text.
        
        Args:
            text: The text to embed
            
        Returns:
            A list of floats representing the embedding vector
        """
        ...
    
    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings for multiple texts.
        
        Batch processing is often more efficient than individual calls.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embedding vectors, one per input text
        """
        ...


# -----------------------------------------------------------------------------
# Summarization
# -----------------------------------------------------------------------------

# Shared system prompt for all LLM-based summarization providers
SUMMARIZATION_SYSTEM_PROMPT = """Summarize this document in under 200 words.

Begin with the subject or topic directly - do not start with meta-phrases like "This document describes..." or "The main purpose is...".

Good: Start with the name of the subject, then say what it is.
Bad: "This document describes..." or "The main purpose is..."

Include what it does, key features, and why someone might find it useful."""

_SPEAKER_RE = re.compile(
    r"^(?:User|Assistant|Human|AI|System)\s*:",
    re.MULTILINE | re.IGNORECASE,
)


def _is_conversation(content: str) -> bool:
    """Detect if content looks like a conversation transcript."""
    return len(_SPEAKER_RE.findall(content[:5000])) >= 4


def build_summarization_prompt(content: str, context: str | None = None) -> str:
    """Build the summarization prompt, optionally including context.

    When context is provided (as topic keywords), it gives the LLM
    thematic context without leaking specific phrases from other summaries.
    Auto-detects conversation content and adjusts instructions accordingly.

    Args:
        content: The document content to summarize
        context: Optional context from related items (summaries of similar items)

    Returns:
        The complete prompt string for the LLM
    """
    if context:
        if _is_conversation(content):
            return f"""Summarize this conversation in under 300 words.

This conversation is part of a collection about: {context}

Summarize only this conversation.

Preserve ALL specific dates, times, names, locations, and factual claims stated by the user. Preserve the chronological order of events.

Conversation:
{content}"""
        else:
            return f"""Summarize this document in under 200 words.

This document is part of a collection about: {context}

Summarize only the document itself.

Begin with the subject or topic directly - do not start with meta-phrases like "This document describes..." or "The main purpose is...".

Include what it does, key features, and why someone might find it useful.

Document:
{content}"""
    else:
        return content


def strip_summary_preamble(text: str) -> str:
    """Remove common LLM preambles from summaries.

    Many models add introductory phrases despite instructions not to.
    This post-processes the output to strip them.
    """
    preambles = [
        r"^here is a summary[^:]*[:.]\s*",
        r"^here is a concise summary[^:]*:\s*",
        r"^here is the summary[^:]*:\s*",
        r"^here's a summary[^:]*:\s*",
        r"^summary:\s*",
        r"^the document describes\s+",
        r"^this document describes\s+",
        r"^the document covers\s+",
        r"^this document covers\s+",
        r"^the main purpose or topic of this document is\s+",
        r"^the main purpose of this document is\s+",
        r"^the purpose of this document is\s+",
        r"^this is a document (about|describing|that)\s+",
        r"^this conversation (is about|covers|discusses)\s+",
        r"^the conversation (is about|covers|discusses)\s+",
        r"^in this conversation,?\s+",
        r"^the user (discusses|talks about|mentions)\s+",
    ]
    result = text
    for pattern in preambles:
        result = re.sub(pattern, "", result, flags=re.IGNORECASE)
    return result


@runtime_checkable
class SummarizationProvider(Protocol):
    """Generates concise summaries of document content.
    
    Summaries are stored alongside items and enable quick recall without
    fetching the original document. They're also used for full-text search.
    
    Example implementation:
        class OpenAISummarization:
            def __init__(self, model: str = "gpt-4o-mini"):
                self.client = OpenAI()
                self.model = model
            
            def summarize(self, content: str, max_length: int = 500) -> str:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "Summarize concisely."},
                        {"role": "user", "content": content}
                    ],
                    max_tokens=max_length
                )
                return response.choices[0].message.content
    """
    
    def summarize(
        self,
        content: str,
        *,
        max_length: int = 500,
        context: str | None = None,
    ) -> str:
        """Generate a summary of the content.

        Args:
            content: The full document content
            max_length: Approximate maximum length in characters
            context: Optional context from related items (for contextual summarization)

        Returns:
            A concise summary of the content
        """
        ...

    def generate(
        self,
        system: str,
        user: str,
        *,
        max_tokens: int = 4096,
    ) -> str | None:
        """Send a raw system+user prompt to the underlying LLM and return text.

        This enables callers (e.g. decomposition) to use the configured LLM
        without introspecting provider internals. Providers that don't wrap
        an LLM (e.g. PassthroughSummarization) should return None.

        Args:
            system: System prompt
            user: User prompt
            max_tokens: Maximum tokens in response

        Returns:
            Generated text, or None if the provider has no LLM capability
        """
        ...


# -----------------------------------------------------------------------------
# Media Description
# -----------------------------------------------------------------------------

@runtime_checkable
class MediaDescriber(Protocol):
    """Generates text descriptions of media files.

    Image describers produce visual descriptions of what's in an image.
    Audio describers produce transcriptions of speech content.

    Returns None for unsupported content types, allowing composites
    to try multiple describers.
    """

    def describe(self, path: str, content_type: str) -> str | None:
        """Generate a text description of a media file.

        Args:
            path: Absolute filesystem path to the media file
            content_type: MIME type (e.g., "image/jpeg", "audio/mpeg")

        Returns:
            Text description/transcription, or None if this describer
            does not support the given content_type.
        """
        ...


# -----------------------------------------------------------------------------
# Content Extraction (OCR, STT)
# -----------------------------------------------------------------------------

@runtime_checkable
class ContentExtractor(Protocol):
    """Recovers actual text content from media files (OCR, speech-to-text).

    Unlike MediaDescriber which generates *descriptions* (semantic),
    ContentExtractor recovers the *original text* present in the media.
    For example: OCR extracts text from a scanned document image,
    STT recovers spoken words from audio.

    Returns None for unsupported content types.
    """

    def extract(self, path: str, content_type: str) -> str | None:
        """Extract text content from a media file.

        Args:
            path: Absolute filesystem path to the media file
            content_type: MIME type (e.g., "image/png", "audio/wav")

        Returns:
            Extracted text, or None if this extractor does not
            support the given content_type.
        """
        ...


# -----------------------------------------------------------------------------
# Analysis (Document Decomposition)
# -----------------------------------------------------------------------------

@dataclass
class AnalysisChunk:
    """A chunk of content for analysis.

    Analyzers receive a sequence of chunks — for version-strings these are
    the individual versions, for monolithic documents a single chunk.
    The analyzer decides its own windowing strategy internally.
    """
    content: str
    tags: dict[str, str]
    index: int  # position in sequence


@runtime_checkable
class AnalyzerProvider(Protocol):
    """Decomposes content into meaningful parts with summaries and tags.

    The default implementation uses token-budgeted sliding windows with
    XML-style target marking, suited to small local models. Alternative
    implementations include single-pass JSON decomposition for large-context
    models, or domain-specific strategies like speech-act recognition.

    Receives chunks (versions of a string, or a single document chunk)
    and returns raw part dicts. Keeper.analyze() handles wrapping these
    into PartInfo, storage, and embedding.
    """

    def analyze(
        self,
        chunks: Iterable[AnalysisChunk],
        guide_context: str = "",
    ) -> list[dict]:
        """Decompose content chunks into parts.

        Args:
            chunks: Content chunks to analyze (versions, sections, or
                    a single chunk for monolithic documents).
                    Iterable to support streaming over large datasets.
            guide_context: Optional tag descriptions for guided decomposition

        Returns:
            List of dicts with "summary", "content", and optionally "tags" keys.
            Empty list on failure.
        """
        ...


# -----------------------------------------------------------------------------
# Tagging
# -----------------------------------------------------------------------------

# Shared system prompt for all LLM-based tagging providers
TAGGING_SYSTEM_PROMPT = """Analyze the document and generate relevant tags as a JSON object.

Generate tags for these categories when applicable:
- content_type: The type of content (e.g., "documentation", "code", "article", "config")
- language: Programming language if code (e.g., "python", "javascript")
- domain: Subject domain (e.g., "authentication", "database", "api", "testing")
- framework: Framework or library if relevant (e.g., "react", "django", "fastapi")

Only include tags that clearly apply. Values should be lowercase.

Respond with a JSON object only, no explanation."""


def parse_tag_json(text: str | None) -> dict[str, str]:
    """Parse JSON tags from LLM response, handling common quirks."""
    if not text:
        return {}
    text = text.strip()
    # Strip markdown code fences
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3].strip()
    try:
        tags = json.loads(text)
        return {str(k): str(v) for k, v in tags.items()}
    except (json.JSONDecodeError, AttributeError):
        return {}


@runtime_checkable
class TaggingProvider(Protocol):
    """Generates structured tags from document content.
    
    Tags enable traditional navigation and filtering. The provider analyzes
    content and returns relevant key-value pairs.
    
    Example implementation:
        class OpenAITagging:
            def __init__(self, model: str = "gpt-4o-mini"):
                self.client = OpenAI()
                self.model = model
            
            def tag(self, content: str) -> dict[str, str]:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[...],
                    response_format={"type": "json_object"}
                )
                return json.loads(response.choices[0].message.content)
    """
    
    def tag(self, content: str) -> dict[str, str]:
        """Generate tags for the content.
        
        Args:
            content: The full document content
            
        Returns:
            Dictionary of tag key-value pairs
            
        Note:
            Keys should be lowercase with underscores (e.g., "content_type").
            Values should be simple strings.
            System tags (keys starting with "_") should not be generated here.
        """
        ...


# -----------------------------------------------------------------------------
# Provider Registry
# -----------------------------------------------------------------------------

class ProviderRegistry:
    """Registry for discovering and instantiating providers.

    Providers are registered by name and can be instantiated from configuration.
    This allows the store configuration (TOML) to specify providers by name
    rather than requiring code changes.

    Example:
        registry = ProviderRegistry()
        registry.register_embedding("sentence-transformers", SentenceTransformerEmbedding)
        registry.register_embedding("openai", OpenAIEmbedding)

        # Later, from config:
        provider = registry.create_embedding("sentence-transformers", {"model": "all-MiniLM-L6-v2"})
    """

    def __init__(self):
        self._embedding_providers: dict[str, type] = {}
        self._summarization_providers: dict[str, type] = {}
        self._tagging_providers: dict[str, type] = {}
        self._document_providers: dict[str, type] = {}
        self._media_providers: dict[str, type] = {}
        self._analyzer_providers: dict[str, type] = {}
        self._content_extractor_providers: dict[str, type] = {}
        self._lazy_loaded = False
    
    def _ensure_providers_loaded(self) -> None:
        """Lazily load all provider modules."""
        if self._lazy_loaded:
            return

        self._lazy_loaded = True

        # Import provider modules to trigger registration
        # These imports are safe - they only register classes, don't instantiate
        try:
            from . import documents
        except ImportError:
            pass  # Document provider might not be available

        try:
            from . import embeddings
        except ImportError:
            pass  # Embedding providers might not be available

        try:
            from . import summarization
        except ImportError:
            pass  # Summarization providers might not be available

        try:
            from . import llm
        except ImportError:
            pass  # LLM providers might not be available

        try:
            from . import mlx
        except ImportError:
            pass  # MLX providers might not be available

        try:
            from .. import analyzers
        except ImportError:
            pass  # Analyzer module might not be available

    # Registration methods

    def register_embedding(self, name: str, provider_class: type) -> None:
        """Register an embedding provider class."""
        self._embedding_providers[name] = provider_class
    
    def register_summarization(self, name: str, provider_class: type) -> None:
        """Register a summarization provider class."""
        self._summarization_providers[name] = provider_class
    
    def register_tagging(self, name: str, provider_class: type) -> None:
        """Register a tagging provider class."""
        self._tagging_providers[name] = provider_class
    
    def register_document(self, name: str, provider_class: type) -> None:
        """Register a document provider class."""
        self._document_providers[name] = provider_class

    def register_media(self, name: str, provider_class: type) -> None:
        """Register a media describer class."""
        self._media_providers[name] = provider_class

    def register_analyzer(self, name: str, provider_class: type) -> None:
        """Register an analyzer provider class."""
        self._analyzer_providers[name] = provider_class

    def register_content_extractor(self, name: str, provider_class: type) -> None:
        """Register a content extractor provider class."""
        self._content_extractor_providers[name] = provider_class
    
    # Factory methods

    @staticmethod
    def _create_provider(kind: str, name: str, providers: dict, params: dict | None):
        """Shared factory logic for all provider types."""
        if name not in providers:
            available = ", ".join(providers.keys()) or "none"
            raise ValueError(
                f"Unknown {kind} provider: '{name}'. "
                f"Available providers: {available}. "
                f"Install missing dependencies or check provider name."
            )
        try:
            return providers[name](**(params or {}))
        except ImportError as e:
            raise RuntimeError(
                f"Failed to create {kind} provider '{name}': {e}\n"
                f"Install required dependencies."
            ) from e
        except Exception as e:
            raise RuntimeError(
                f"Failed to create {kind} provider '{name}': {e}"
            ) from e

    def create_embedding(self, name: str, params: dict | None = None) -> EmbeddingProvider:
        """Create an embedding provider instance."""
        self._ensure_providers_loaded()
        return self._create_provider("embedding", name, self._embedding_providers, params)

    def create_summarization(self, name: str, params: dict | None = None) -> SummarizationProvider:
        """Create a summarization provider instance."""
        self._ensure_providers_loaded()
        return self._create_provider("summarization", name, self._summarization_providers, params)

    def create_tagging(self, name: str, params: dict | None = None) -> TaggingProvider:
        """Create a tagging provider instance."""
        self._ensure_providers_loaded()
        return self._create_provider("tagging", name, self._tagging_providers, params)

    def create_media(self, name: str, params: dict | None = None) -> MediaDescriber:
        """Create a media describer instance."""
        self._ensure_providers_loaded()
        return self._create_provider("media", name, self._media_providers, params)

    def create_analyzer(self, name: str, params: dict | None = None) -> AnalyzerProvider:
        """Create an analyzer provider instance."""
        self._ensure_providers_loaded()
        return self._create_provider("analyzer", name, self._analyzer_providers, params)

    def create_content_extractor(self, name: str, params: dict | None = None) -> ContentExtractor:
        """Create a content extractor provider instance."""
        self._ensure_providers_loaded()
        return self._create_provider("content_extractor", name, self._content_extractor_providers, params)

    def create_document(self, name: str, params: dict | None = None) -> DocumentProvider:
        """Create a document provider instance."""
        self._ensure_providers_loaded()
        return self._create_provider("document", name, self._document_providers, params)
    
    # Introspection
    
    def list_embedding_providers(self) -> list[str]:
        """List registered embedding provider names."""
        return list(self._embedding_providers.keys())
    
    def list_summarization_providers(self) -> list[str]:
        """List registered summarization provider names."""
        return list(self._summarization_providers.keys())
    
    def list_tagging_providers(self) -> list[str]:
        """List registered tagging provider names."""
        return list(self._tagging_providers.keys())
    
    def list_document_providers(self) -> list[str]:
        """List registered document provider names."""
        return list(self._document_providers.keys())

    def list_media_providers(self) -> list[str]:
        """List registered media describer names."""
        return list(self._media_providers.keys())

    def list_content_extractor_providers(self) -> list[str]:
        """List registered content extractor names."""
        return list(self._content_extractor_providers.keys())


# Global registry instance
# Concrete providers register themselves on import
_registry = ProviderRegistry()


def get_registry() -> ProviderRegistry:
    """Get the global provider registry."""
    return _registry
