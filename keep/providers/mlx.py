"""MLX providers for Apple Silicon.

MLX is Apple's ML framework optimized for Apple Silicon. These providers
run entirely locally with no API keys required.

Requires: pip install mlx-lm mlx
"""

from .base import (
    get_registry,
    build_summarization_prompt,
    SUMMARIZATION_SYSTEM_PROMPT,
    TAGGING_SYSTEM_PROMPT,
    parse_tag_json,
)


class MLXEmbedding:
    """Embedding provider using MPS (Metal) acceleration on Apple Silicon.

    Uses sentence-transformer models with GPU acceleration via Metal Performance Shaders.

    Requires: pip install mlx sentence-transformers
    """

    def __init__(self, model: str = "all-MiniLM-L6-v2"):
        """Initialize.

        Args:
        model: Model name from sentence-transformers hub.
               Default: all-MiniLM-L6-v2 (384 dims, fast, no auth required).
        """
        try:
            import mlx.core as mx
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise RuntimeError(
                "MLXEmbedding requires 'mlx' and 'sentence-transformers'. "
                "Install with: pip install mlx sentence-transformers"
            )

        self.model_name = model

        # Check if model is already cached locally to avoid network calls
        local_only = False
        try:
            from huggingface_hub import try_to_load_from_cache
            repo_id = model if "/" in model else f"sentence-transformers/{model}"
            cached = try_to_load_from_cache(repo_id, "config.json")
            local_only = cached is not None
        except ImportError:
            pass

        if not local_only:
            import logging
            import sys
            logging.getLogger(__name__).info("Downloading embedding model '%s' (first use)...", model)
            print(f"Downloading embedding model '{model}' (first use)...", file=sys.stderr)

        # Use MPS (Metal) for GPU acceleration on Apple Silicon
        self._model = SentenceTransformer(model, device="mps", local_files_only=local_only)

        self._dimension: int | None = None
    
    @property
    def dimension(self) -> int:
        """Get embedding dimension from the model."""
        if self._dimension is None:
            self._dimension = self._model.get_sentence_embedding_dimension()
        return self._dimension
    
    def embed(self, text: str) -> list[float]:
        """Generate embedding for a single text."""
        embedding = self._model.encode(text, convert_to_numpy=True)
        return embedding.tolist()
    
    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings for multiple texts."""
        embeddings = self._model.encode(texts, convert_to_numpy=True)
        return embeddings.tolist()


class MLXSummarization:
    """Summarization provider using MLX-LM on Apple Silicon.

    Runs local LLMs optimized for Apple Silicon. No API key required.

    Requires: pip install mlx-lm
    """

    def __init__(
        self,
        model: str = "mlx-community/Llama-3.2-3B-Instruct-4bit",
        max_tokens: int = 300,
    ):
        """Initialize.

        Args:
        model: Model name from mlx-community hub or local path.
               Good options for summarization:
               - mlx-community/Llama-3.2-3B-Instruct-4bit (fast, small)
               - mlx-community/Llama-3.2-8B-Instruct-4bit (better quality)
               - mlx-community/Mistral-7B-Instruct-v0.3-4bit (good balance)
               - mlx-community/Phi-3.5-mini-instruct-4bit (very fast)
        max_tokens: Maximum tokens in generated summary.
        """
        try:
            from mlx_lm import load
        except ImportError:
            raise RuntimeError(
                "MLXSummarization requires 'mlx-lm'. "
                "Install with: pip install mlx-lm"
            )
        
        self.model_name = model
        self.max_tokens = max_tokens

        # Check if model is already cached
        _downloading = False
        try:
            from huggingface_hub import try_to_load_from_cache
            cached = try_to_load_from_cache(model, "config.json")
            _downloading = cached is None
        except ImportError:
            pass

        if _downloading:
            import logging
            import sys
            logging.getLogger(__name__).info("Downloading MLX model '%s' (first use)...", model)
            print(f"Downloading MLX model '{model}' (first use)...", file=sys.stderr)

        self._model, self._tokenizer = load(model)

    def summarize(
        self,
        content: str,
        *,
        max_length: int = 500,
        context: str | None = None,
    ) -> str:
        """Generate a summary using MLX-LM."""
        # MLX models have smaller context windows (4k-8k)
        truncated = content[:12000] if len(content) > 12000 else content
        prompt = build_summarization_prompt(truncated, context)
        result = self.generate(SUMMARIZATION_SYSTEM_PROMPT, prompt)
        return result if result else truncated[:max_length]

    def generate(
        self,
        system: str,
        user: str,
        *,
        max_tokens: int = 4096,
    ) -> str | None:
        """Send a raw prompt to MLX-LM and return generated text."""
        from mlx_lm import generate

        if hasattr(self._tokenizer, "apply_chat_template"):
            messages = [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ]
            prompt = self._tokenizer.apply_chat_template(
                messages, tokenize=False, add_generation_prompt=True,
            )
        else:
            prompt = f"{system}\n\n{user}"

        response = generate(
            self._model,
            self._tokenizer,
            prompt=prompt,
            max_tokens=max_tokens,
            verbose=False,
        )
        return response.strip()


class MLXTagging:
    """Tagging provider using MLX-LM on Apple Silicon.
    
    Uses local LLMs to generate structured tags. No API key required.
    
    Requires: pip install mlx-lm
    """
    
    def __init__(
        self,
        model: str = "mlx-community/Llama-3.2-3B-Instruct-4bit",
        max_tokens: int = 150,
    ):
        """Initialize.

        Args:
        model: Model name from mlx-community hub
        max_tokens: Maximum tokens in generated response.
        """
        try:
            from mlx_lm import load
        except ImportError:
            raise RuntimeError(
                "MLXTagging requires 'mlx-lm'. "
                "Install with: pip install mlx-lm"
            )
        
        self.model_name = model
        self.max_tokens = max_tokens

        # Check if model is already cached
        _downloading = False
        try:
            from huggingface_hub import try_to_load_from_cache
            cached = try_to_load_from_cache(model, "config.json")
            _downloading = cached is None
        except ImportError:
            pass

        if _downloading:
            import logging
            import sys
            logging.getLogger(__name__).info("Downloading MLX model '%s' (first use)...", model)
            print(f"Downloading MLX model '{model}' (first use)...", file=sys.stderr)

        self._model, self._tokenizer = load(model)

    def tag(self, content: str) -> dict[str, str]:
        """Generate tags using MLX-LM."""
        from mlx_lm import generate

        truncated = content[:8000] if len(content) > 8000 else content

        if hasattr(self._tokenizer, "apply_chat_template"):
            messages = [
                {"role": "system", "content": TAGGING_SYSTEM_PROMPT},
                {"role": "user", "content": truncated},
            ]
            prompt = self._tokenizer.apply_chat_template(
                messages, tokenize=False, add_generation_prompt=True,
            )
        else:
            prompt = f"{TAGGING_SYSTEM_PROMPT}\n\nDocument:\n{truncated}\n\nJSON:"

        response = generate(
            self._model, self._tokenizer,
            prompt=prompt, max_tokens=self.max_tokens, verbose=False,
        )
        return parse_tag_json(response)


class MLXVisionDescriber:
    """Image description using MLX-VLM on Apple Silicon.

    Uses local vision-language models to generate text descriptions of images.
    No API key required.

    Requires: pip install mlx-vlm
    """

    IMAGE_PROMPT = (
        "Describe this image in detail. Include the subject, setting, "
        "colors, composition, and any text visible in the image. "
        "Be specific and factual."
    )

    def __init__(
        self,
        model: str = "mlx-community/Qwen2-VL-2B-Instruct-4bit",
        max_tokens: int = 300,
    ):
        try:
            from mlx_vlm import load as vlm_load
        except ImportError:
            raise RuntimeError(
                "MLXVisionDescriber requires 'mlx-vlm'. "
                "Install with: pip install mlx-vlm"
            )

        self.model_name = model
        self.max_tokens = max_tokens

        _downloading = False
        try:
            from huggingface_hub import try_to_load_from_cache
            cached = try_to_load_from_cache(model, "config.json")
            _downloading = cached is None
        except ImportError:
            pass

        if _downloading:
            import logging
            import sys
            logging.getLogger(__name__).info("Downloading MLX vision model '%s' (first use)...", model)
            print(f"Downloading MLX vision model '{model}' (first use)...", file=sys.stderr)

        self._model, self._processor = vlm_load(model)

    def describe(self, path: str, content_type: str) -> str | None:
        """Describe an image using MLX-VLM."""
        if not content_type.startswith("image/"):
            return None

        from mlx_vlm import generate as vlm_generate

        response = vlm_generate(
            self._model,
            self._processor,
            prompt=self.IMAGE_PROMPT,
            image=path,
            max_tokens=self.max_tokens,
            verbose=False,
        )

        return response.strip() if response else None


class MLXWhisperDescriber:
    """Audio transcription using MLX-Whisper on Apple Silicon.

    Uses local Whisper models to transcribe speech to text.
    No API key required.

    Requires: pip install mlx-whisper
    """

    def __init__(
        self,
        model: str = "mlx-community/whisper-large-v3-turbo",
    ):
        try:
            import mlx_whisper  # noqa: F401
        except ImportError:
            raise RuntimeError(
                "MLXWhisperDescriber requires 'mlx-whisper'. "
                "Install with: pip install mlx-whisper"
            )

        self.model_name = model

    def describe(self, path: str, content_type: str) -> str | None:
        """Transcribe audio using MLX-Whisper."""
        if not content_type.startswith("audio/"):
            return None

        import mlx_whisper

        result = mlx_whisper.transcribe(
            path,
            path_or_hf_repo=self.model_name,
        )

        text = result.get("text", "").strip()
        return text if text else None


class MLXContentExtractor:
    """OCR content extraction using MLX-VLM on Apple Silicon.

    Uses GLM-OCR to extract text from document images. Unlike MLXVisionDescriber
    which generates semantic descriptions, this recovers the actual text content.

    Requires: pip install mlx-vlm
    """

    OCR_PROMPT = "Extract all text from this document image exactly as written."

    def __init__(
        self,
        model: str = "mlx-community/GLM-OCR-bf16",
        max_tokens: int = 2000,
    ):
        try:
            from mlx_vlm import load as vlm_load
        except ImportError:
            raise RuntimeError(
                "MLXContentExtractor requires 'mlx-vlm'. "
                "Install with: pip install mlx-vlm"
            )

        self.model_name = model
        self.max_tokens = max_tokens

        _downloading = False
        try:
            from huggingface_hub import try_to_load_from_cache
            cached = try_to_load_from_cache(model, "config.json")
            _downloading = cached is None
        except ImportError:
            pass

        if _downloading:
            import logging
            import sys
            logging.getLogger(__name__).info("Downloading MLX OCR model '%s' (first use)...", model)
            print(f"Downloading MLX OCR model '{model}' (first use)...", file=sys.stderr)

        self._model, self._processor = vlm_load(model)

    def extract(self, path: str, content_type: str) -> str | None:
        """Extract text from an image using OCR."""
        if not content_type.startswith("image/"):
            return None

        from mlx_vlm import generate as vlm_generate

        response = vlm_generate(
            self._model,
            self._processor,
            prompt=self.OCR_PROMPT,
            image=path,
            max_tokens=self.max_tokens,
            verbose=False,
        )

        text = response.strip() if response else ""
        return text if len(text) > 10 else None


class MLXMediaDescriber:
    """Combined media describer for Apple Silicon.

    Handles both image description (via mlx-vlm) and audio transcription
    (via mlx-whisper). Sub-providers are created lazily — only loaded when
    first needed for that content type.

    Requires: pip install mlx-vlm (images) and/or mlx-whisper (audio)
    """

    def __init__(
        self,
        vision_model: str = "mlx-community/Qwen2-VL-2B-Instruct-4bit",
        whisper_model: str = "mlx-community/whisper-large-v3-turbo",
        max_tokens: int = 300,
    ):
        self._vision_model = vision_model
        self._whisper_model = whisper_model
        self._max_tokens = max_tokens
        self._vision: MLXVisionDescriber | None = None
        self._whisper: MLXWhisperDescriber | None = None
        self._vision_checked = False
        self._whisper_checked = False

    def describe(self, path: str, content_type: str) -> str | None:
        """Describe media using the appropriate sub-provider."""
        if content_type.startswith("image/"):
            return self._describe_image(path, content_type)
        elif content_type.startswith("audio/"):
            return self._describe_audio(path, content_type)
        return None

    def _describe_image(self, path: str, content_type: str) -> str | None:
        if not self._vision_checked:
            self._vision_checked = True
            try:
                self._vision = MLXVisionDescriber(
                    model=self._vision_model,
                    max_tokens=self._max_tokens,
                )
            except RuntimeError:
                pass  # mlx-vlm not installed
        if self._vision:
            return self._vision.describe(path, content_type)
        return None

    def _describe_audio(self, path: str, content_type: str) -> str | None:
        if not self._whisper_checked:
            self._whisper_checked = True
            try:
                self._whisper = MLXWhisperDescriber(
                    model=self._whisper_model,
                )
            except RuntimeError:
                pass  # mlx-whisper not installed
        if self._whisper:
            return self._whisper.describe(path, content_type)
        return None


def is_apple_silicon() -> bool:
    """Check if running on Apple Silicon."""
    import platform
    return platform.system() == "Darwin" and platform.machine() == "arm64"


# Register providers (only on Apple Silicon)
if is_apple_silicon():
    _registry = get_registry()
    _registry.register_embedding("mlx", MLXEmbedding)
    _registry.register_summarization("mlx", MLXSummarization)
    _registry.register_tagging("mlx", MLXTagging)
    _registry.register_media("mlx", MLXMediaDescriber)
    _registry.register_content_extractor("mlx", MLXContentExtractor)
