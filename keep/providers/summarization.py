"""Default summarization providers.

Simple, zero-dependency summarizers for getting started.
"""

from .base import get_registry


class TruncationSummarizer:
    """Simple summarizer that truncates content to a max length.
    
    Zero dependencies. Good enough to get started; replace with
    LLM-based summarization when quality matters.
    """
    
    def __init__(self, max_length: int = 500, suffix: str = "..."):
        """Initialize.

        Args:
        max_length: Maximum summary length in characters
        suffix: Suffix to add when truncated.
        """
        self.max_length = max_length
        self.suffix = suffix
    
    def summarize(
        self,
        content: str,
        *,
        max_length: int | None = None,
        context: str | None = None,
    ) -> str:
        """Summarize by taking first N characters.

        Tries to break at word boundaries. Context is ignored (non-LLM provider).
        """
        limit = max_length or self.max_length

        if len(content) <= limit:
            return content.strip()

        # Find last space before limit to break at word boundary
        truncated = content[:limit]
        last_space = truncated.rfind(' ')

        if last_space > limit * 0.7:  # Don't break too early
            truncated = truncated[:last_space]

        return truncated.strip() + self.suffix

    def generate(self, system: str, user: str, *, max_tokens: int = 4096) -> str | None:
        """Non-LLM provider — return None."""
        return None


class FirstParagraphSummarizer:
    """Summarizer that extracts the first paragraph or meaningful chunk.
    
    Better than pure truncation for documents with structure.
    """
    
    def __init__(self, max_length: int = 500, suffix: str = "..."):
        self.max_length = max_length
        self.suffix = suffix
    
    def summarize(
        self,
        content: str,
        *,
        max_length: int | None = None,
        context: str | None = None,
    ) -> str:
        """Extract first paragraph, falling back to truncation. Context is ignored."""
        limit = max_length or self.max_length

        # Strip leading whitespace and find first paragraph
        content = content.strip()

        # Look for paragraph break (double newline)
        para_end = content.find('\n\n')
        if para_end > 0 and para_end < limit:
            first_para = content[:para_end].strip()
            if len(first_para) > 50:  # Paragraph is meaningful
                return first_para

        # Fall back to truncation
        if len(content) <= limit:
            return content

        truncated = content[:limit]
        last_space = truncated.rfind(' ')
        if last_space > limit * 0.7:
            truncated = truncated[:last_space]

        return truncated.strip() + self.suffix

    def generate(self, system: str, user: str, *, max_tokens: int = 4096) -> str | None:
        """Non-LLM provider — return None."""
        return None


# Register providers
# Note: "passthrough" is registered in providers/llm.py (PassthroughSummarization)
_registry = get_registry()
_registry.register_summarization("truncate", TruncationSummarizer)
_registry.register_summarization("first_paragraph", FirstParagraphSummarizer)
