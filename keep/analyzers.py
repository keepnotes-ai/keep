"""Analyzer implementations for document decomposition.

Two implementations:
- SlidingWindowAnalyzer (default): Token-budgeted sliding windows with
  XML-style target marking. Works well with small local models.
- SinglePassAnalyzer: Single-pass LLM decomposition with JSON output.
  Better for large-context models.
"""

import hashlib
import json
import logging
import re
from collections.abc import Iterable

from .providers.base import AnalysisChunk, AnalyzerProvider, get_registry, strip_summary_preamble

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Token estimation
# ---------------------------------------------------------------------------

def _estimate_tokens(text: str) -> int:
    """Rough token estimate — chars / 4 for English text."""
    return len(text) // 4


# ---------------------------------------------------------------------------
# Sliding-window output parser
# ---------------------------------------------------------------------------

# Preamble patterns that small models emit despite instructions
_PREAMBLE_RE = re.compile(
    r"^here are the (?:significant )?(?:developments|observations|summaries)[^:]*:\s*$",
    re.IGNORECASE,
)


def _parse_parts(text: str) -> list[dict]:
    """Parse one-summary-per-line output from LLM.

    Handles common LLM quirks: preamble lines, leaked XML tags,
    EMPTY sentinel, and very short lines.
    """
    if not text:
        return []

    results = []
    for line in text.strip().splitlines():
        line = strip_summary_preamble(line.strip())
        # Strip leaked XML tags
        line = re.sub(r"</?(?:analyze|content)>", "", line).strip()
        if not line or line == "EMPTY" or len(line) < 20:
            continue
        if _PREAMBLE_RE.match(line):
            continue
        results.append({"summary": line})
    return results


# ---------------------------------------------------------------------------
# Default analysis prompt — fallback when no .prompt/analyze/* doc matches
# ---------------------------------------------------------------------------

DEFAULT_ANALYSIS_PROMPT = """Analyze the evolution of a conversation. Entries are dated and wrapped in <content> tags. Only analyze content inside <analyze> tags.

Write ONE LINE per significant development. Each line should describe what specifically changed or was decided, in plain language.

Rules:
- One observation per line, no numbering, no bullets, no preamble
- Synthesize in your own words — never copy or quote the original text
- Be specific: name the actual thing that changed, not abstract categories
- Do not start lines with category labels like "Decision:", "Theme:", "Turning point:"
- Do not include XML tags in your output
- Skip greetings, acknowledgments, and routine exchanges
- If nothing noteworthy: EMPTY"""


# ---------------------------------------------------------------------------
# Model → effective context budget mapping
# ---------------------------------------------------------------------------

# Effective analysis budget per model.  This is NOT the raw context window —
# it's how much content the model can usefully analyze in a single window
# while still producing quality output (synthesis, not paraphrase).
# Small models degrade fast; cloud models can handle much more.
MODEL_BUDGETS: dict[str, int] = {
    # Ollama / local (free)
    "llama3.2:1b":       1500,
    "llama3.2:3b":       3000,
    "llama3.2":          3000,
    "llama3.1:8b":       6000,
    "llama3.1":          6000,
    "qwen2.5:3b":        3000,
    "qwen2.5:7b":        6000,
    "qwen2.5:14b":       8000,
    "mistral:7b":        6000,
    "gemma2:9b":         6000,
    # OpenAI
    "gpt-5-nano":        4000,
    "gpt-4o-mini":       6000,
    "gpt-5-mini":       12000,
    "gpt-4.1-mini":     12000,
    "gpt-4.1":          16000,
    "gpt-4o":           16000,
    "gpt-5":            16000,
    # Anthropic
    "claude-3-haiku-20240307":    8000,
    "claude-3-5-haiku-20241022": 12000,
    "claude-haiku-4-5-20251001": 12000,
    "claude-sonnet-4-6":         16000,
    "claude-opus-4-6":           16000,
    # Google Gemini
    "gemini-2.0-flash":          8000,
    "gemini-2.5-flash-lite":     8000,
    "gemini-2.5-flash":         12000,
    "gemini-2.5-pro":           16000,
}

# Fallback budgets by provider name (when model isn't in the table)
_PROVIDER_BUDGET_FALLBACK: dict[str, int] = {
    "ollama": 3000,
    "mlx": 3000,
    "anthropic": 12000,
    "openai": 12000,
    "gemini": 10000,
    "truncate": 6000,
}

# Default when nothing matches
_DEFAULT_BUDGET = 6000


def get_budget_for_model(model: str, provider: str = "") -> int:
    """Look up effective analysis budget for a model, with fallback."""
    if model in MODEL_BUDGETS:
        return MODEL_BUDGETS[model]
    # Prefix match (e.g. "llama3.2:3b-q4_0" → "llama3.2:3b")
    for known, budget in MODEL_BUDGETS.items():
        if model.startswith(known):
            return budget
    budget = _PROVIDER_BUDGET_FALLBACK.get(provider, _DEFAULT_BUDGET)
    if model:
        logger.info(
            "Unknown model %r for analyzer budget; using %d (provider=%s). "
            "Override with [analyzer] context_budget in keep.toml.",
            model, budget, provider or "unknown",
        )
    return budget


# ---------------------------------------------------------------------------
# SlidingWindowAnalyzer — default
# ---------------------------------------------------------------------------

class SlidingWindowAnalyzer:
    """Token-budgeted sliding-window decomposition.

    Processes chunks in windows sized to the model's context budget.
    Each window uses XML-style tags to mark which chunks are analysis
    targets vs. context-only. Deduplicates by content hash across windows.

    Works well with small local models (Ollama, MLX) that have limited
    context windows.
    """

    def __init__(
        self,
        provider=None,
        context_budget: int = 12000,
        target_ratio: float = 0.6,
        prompt: str | None = None,
    ):
        """Initialize.

        Args:
        provider: A SummarizationProvider with generate() support.
        context_budget: Total token budget per window.
        target_ratio: Fraction of budget allocated to target chunks (vs context).
        prompt: Fixed prompt text to use. If None, uses DEFAULT_ANALYSIS_PROMPT
            (override with prompt_override in analyze()).
        """
        self._provider = provider
        self._context_budget = context_budget
        self._target_ratio = target_ratio
        self._fixed_prompt = prompt

    def _resolve_prompt(self, prompt_override: str | None = None) -> str:
        """Return the system prompt text for this analysis call."""
        if prompt_override:
            return prompt_override
        if self._fixed_prompt:
            return self._fixed_prompt
        return DEFAULT_ANALYSIS_PROMPT

    def analyze(
        self,
        chunks: Iterable[AnalysisChunk],
        guide_context: str = "",
        prompt_override: str | None = None,
    ) -> list[dict]:
        """Decompose content chunks into parts using sliding windows."""
        chunk_list = list(chunks)
        if not chunk_list:
            return []

        system_prompt = self._resolve_prompt(prompt_override)

        total_tokens = sum(_estimate_tokens(c.content) for c in chunk_list)

        # Fits in one window — single-pass
        if total_tokens <= self._context_budget:
            return self._single_pass(chunk_list, guide_context, system_prompt)

        target_budget = int(self._context_budget * self._target_ratio)
        context_budget_per_side = (self._context_budget - target_budget) // 2
        all_parts = []
        seen_hashes: set[str] = set()
        pos = 0

        while pos < len(chunk_list):
            # Collect target chunks up to target_budget
            target_end = pos
            target_tokens = 0
            while target_end < len(chunk_list):
                chunk_tokens = _estimate_tokens(chunk_list[target_end].content)
                if target_tokens + chunk_tokens > target_budget and target_end > pos:
                    break  # budget exceeded (but always include at least one)
                target_tokens += chunk_tokens
                target_end += 1

            # Add context before (scan backwards from pos)
            ctx_before = pos
            ctx_before_tokens = 0
            while ctx_before > 0:
                chunk_tokens = _estimate_tokens(chunk_list[ctx_before - 1].content)
                if ctx_before_tokens + chunk_tokens > context_budget_per_side:
                    break
                ctx_before -= 1
                ctx_before_tokens += chunk_tokens

            # Add context after (scan forwards from target_end)
            ctx_after = target_end
            ctx_after_tokens = 0
            while ctx_after < len(chunk_list):
                chunk_tokens = _estimate_tokens(chunk_list[ctx_after].content)
                if ctx_after_tokens + chunk_tokens > context_budget_per_side:
                    break
                ctx_after += 1
                ctx_after_tokens += chunk_tokens

            # Build window and call LLM
            window = chunk_list[ctx_before:ctx_after]
            target_start_in_window = pos - ctx_before
            target_end_in_window = target_end - ctx_before

            raw = self._analyze_window(
                window, target_start_in_window, target_end_in_window,
                guide_context, system_prompt,
            )

            # Dedup across windows by summary hash
            for part in raw:
                h = hashlib.md5(part["summary"].encode()).hexdigest()
                if h not in seen_hashes:
                    seen_hashes.add(h)
                    all_parts.append(part)

            pos = target_end

        return all_parts

    def _single_pass(self, chunks: list[AnalysisChunk], guide_context: str,
                     system_prompt: str) -> list[dict]:
        """Content fits in one window — send all as targets."""
        provider = self._provider
        if provider is None:
            return []

        if hasattr(provider, '_provider') and provider._provider is not None:
            provider = provider._provider

        content = "\n\n---\n\n".join(c.content for c in chunks)
        truncated = content[:80000] if len(content) > 80000 else content

        user_prompt = f"<content>\n<analyze>\n{truncated}\n</analyze>\n</content>"
        if guide_context:
            user_prompt = f"{guide_context}\n\n---\n\n{user_prompt}"

        try:
            result = provider.generate(system_prompt, user_prompt, max_tokens=4096)
            if result:
                return _parse_parts(result)
            logger.warning("Provider returned no result for analysis")
        except Exception as e:
            logger.warning("Analysis failed: %s", e)
        return []

    def _analyze_window(
        self,
        window: list[AnalysisChunk],
        target_start: int,
        target_end: int,
        guide_context: str,
        system_prompt: str,
    ) -> list[dict]:
        """Analyze a single window with XML-tagged target marking."""
        provider = self._provider
        if provider is None:
            return []

        if hasattr(provider, '_provider') and provider._provider is not None:
            provider = provider._provider

        prompt = self._build_window_prompt(window, target_start, target_end)

        if guide_context:
            prompt = f"{guide_context}\n\n---\n\n{prompt}"

        try:
            result = provider.generate(system_prompt, prompt, max_tokens=4096)
            if result:
                return _parse_parts(result)
            logger.warning("Sliding window: provider returned no result")
            return []
        except Exception as e:
            logger.warning("Sliding window LLM call failed: %s", e)
            return []

    @staticmethod
    def _build_window_prompt(
        window: list[AnalysisChunk],
        target_start: int,
        target_end: int,
    ) -> str:
        """Build XML-tagged prompt with <content> and <analyze> markers."""
        parts = ["<content>"]

        for i, chunk in enumerate(window):
            if i == target_start:
                parts.append("<analyze>")
            parts.append(chunk.content)
            if i == target_end - 1:
                parts.append("</analyze>")

        parts.append("</content>")
        return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# SinglePassAnalyzer — legacy, available as "single-pass" in registry
# ---------------------------------------------------------------------------

DECOMPOSITION_SYSTEM_PROMPT = """You are a document analysis assistant. Your task is to decompose a document into its meaningful structural sections.

For each section, provide:
- "summary": A concise summary of the section (1-3 sentences)
- "content": The exact text of the section
- "tags": A dict of relevant tags for this section (optional)

Return a JSON array of section objects. Example:
```json
[
  {"summary": "Introduction and overview of the topic", "content": "The text of section 1...", "tags": {"topic": "overview"}},
  {"summary": "Detailed analysis of the main argument", "content": "The text of section 2...", "tags": {"topic": "analysis"}}
]
```

Guidelines:
- Identify natural section boundaries (headings, topic shifts, structural breaks)
- Each section should be a coherent unit of meaning
- Preserve the original text exactly in the "content" field
- Keep summaries concise but descriptive
- Tags should capture the essence of each section's subject matter
- Return valid JSON only, no commentary outside the JSON array"""


def _parse_decomposition_json(text: str) -> list[dict]:
    """Parse JSON from LLM decomposition output.

    Handles code fences, wrapper objects, and direct JSON arrays.
    """
    if not text:
        return []

    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Failed to parse decomposition JSON")
        return []

    if isinstance(data, dict):
        for key in ("sections", "parts", "chunks", "result", "data"):
            if key in data and isinstance(data[key], list):
                data = data[key]
                break
        else:
            return []

    if not isinstance(data, list):
        return []

    result = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        if not entry.get("summary") and not entry.get("content"):
            continue
        section = {
            "summary": str(entry.get("summary", "")),
            "content": str(entry.get("content", "")),
        }
        if entry.get("tags") and isinstance(entry["tags"], dict):
            section["tags"] = {str(k): str(v) for k, v in entry["tags"].items()}
        result.append(section)

    return result


class SinglePassAnalyzer:
    """Single-pass LLM decomposition with JSON output.

    Concatenates all chunks, sends to the configured summarization provider's
    generate() method, and parses the resulting JSON into part dicts.
    Better for large-context models that can handle the full document at once.
    """

    def __init__(self, provider=None):
        self._provider = provider

    def analyze(
        self,
        chunks: Iterable[AnalysisChunk],
        guide_context: str = "",
    ) -> list[dict]:
        """Decompose content chunks into parts."""
        chunk_list = list(chunks)
        content = "\n\n---\n\n".join(c.content for c in chunk_list)
        return self._call_llm(content, guide_context)

    def _call_llm(self, content: str, guide_context: str = "") -> list[dict]:
        """Call the LLM to decompose content into sections."""
        provider = self._provider
        if provider is None:
            return []

        if hasattr(provider, '_provider') and provider._provider is not None:
            provider = provider._provider

        truncated = content[:80000] if len(content) > 80000 else content

        user_prompt = truncated
        if guide_context:
            user_prompt = (
                f"Decompose this document into meaningful sections.\n\n"
                f"Use these tag definitions to guide your tagging:\n\n"
                f"{guide_context}\n\n"
                f"---\n\n"
                f"Document to analyze:\n\n{truncated}"
            )

        try:
            result = provider.generate(
                DECOMPOSITION_SYSTEM_PROMPT,
                user_prompt,
                max_tokens=4096,
            )
            if result:
                return _parse_decomposition_json(result)
            logger.warning(
                "Provider %s returned no result for decomposition",
                type(provider).__name__,
            )
            return []
        except Exception as e:
            logger.warning("LLM decomposition failed: %s", e)
            return []


# ---------------------------------------------------------------------------
# TagClassifier — data-driven post-analysis classification
# ---------------------------------------------------------------------------

# Regex to parse classifier output lines like:
#   3: act=commitment(0.9) status=open(0.85)
# Tolerant: handles "NUMBER:" prefix echo from small models
_CLASSIFY_LINE_RE = re.compile(
    r"^(?:NUMBER:\s*)?(\d+):\s*(.*)$"
)
# Tolerant: handles spaces around parens (e.g. "act=assertion (0.9)")
_TAG_ASSIGNMENT_RE = re.compile(
    r"(\w+)=(\w+)\s*\(\s*([0-9.]+)\s*\)"
)
# Extract ## Prompt section from document content
_PROMPT_SECTION_RE = re.compile(
    r"^## Prompt\s*\n(.*?)(?=^## |\Z)",
    re.MULTILINE | re.DOTALL,
)


def extract_prompt_section(content: str) -> str:
    """Extract ## Prompt section from document content, if present."""
    m = _PROMPT_SECTION_RE.search(content)
    return m.group(1).strip() if m else ""


class TagClassifier:
    """Data-driven tag classification for analyzed parts.

    Reads constrained tag specifications from the store and builds a
    classification prompt dynamically. Each part summary is classified
    against the taxonomy; tags are applied only above a confidence threshold.

    This is a post-processing step — run after SlidingWindowAnalyzer
    produces parts, before tags are written to the store.
    """

    def __init__(
        self,
        provider=None,
        confidence_threshold: float = 0.7,
    ):
        """Initialize.

        Args:
        provider: A SummarizationProvider with generate() support.
        confidence_threshold: Minimum confidence (0-1) to apply a tag.
        """
        self._provider = provider
        self._confidence_threshold = confidence_threshold
        self._tag_specs: list[dict] | None = None

    @staticmethod
    def _extract_prompt_section(content: str) -> str:
        """Extract ## Prompt section from document content, if present."""
        return extract_prompt_section(content)

    def load_specs(self, keeper) -> list[dict]:
        """Load constrained tag specifications from the store.

        Scans for .tag/* documents with _constrained=true, then loads
        their sub-documents to build the classification taxonomy.

        If a document contains a ``## Prompt`` section, that section is
        extracted and used as the classifier-facing description (the
        ``prompt`` field). The human-readable description (``description``)
        is always taken from the document summary.

        Args:
            keeper: A Keeper instance to read tag specs from.

        Returns:
            List of tag spec dicts, each with:
              - key: tag name (e.g. "act")
              - description: from the parent doc summary
              - prompt: from the parent doc's ## Prompt section (or "")
              - values: list of {value, description, prompt} dicts
        """
        specs = []
        # Find all .tag/* parent docs
        doc_coll = keeper._resolve_doc_collection()
        tag_docs = keeper._document_store.query_by_id_prefix(doc_coll, ".tag/")

        # Group: parent docs vs value docs
        parents = {}  # key -> record
        children = {}  # key -> [record, ...]
        for rec in tag_docs:
            doc_id = rec.id if hasattr(rec, 'id') else rec.get("id", "")
            # .tag/act -> parent; .tag/act/commitment -> child
            parts = doc_id.split("/")
            if len(parts) == 2:
                # Parent: .tag/KEY
                key = parts[1]
                parents[key] = rec
            elif len(parts) == 3:
                # Child: .tag/KEY/VALUE
                key = parts[1]
                children.setdefault(key, []).append(rec)

        for key, parent_rec in parents.items():
            tags = parent_rec.tags if hasattr(parent_rec, 'tags') else parent_rec.get("tags", {})
            if tags.get("_constrained") != "true":
                continue

            parent_summary = parent_rec.summary if hasattr(parent_rec, 'summary') else parent_rec.get("summary", "")
            parent_prompt = self._extract_prompt_section(parent_summary)

            # Build value list from children
            values = []
            for child_rec in children.get(key, []):
                child_id = child_rec.id if hasattr(child_rec, 'id') else child_rec.get("id", "")
                value_name = child_id.split("/")[-1]
                summary = child_rec.summary if hasattr(child_rec, 'summary') else child_rec.get("summary", "")
                child_prompt = self._extract_prompt_section(summary)
                values.append({
                    "value": value_name,
                    "description": summary,
                    "prompt": child_prompt,
                })

            specs.append({
                "key": key,
                "description": parent_summary,
                "prompt": parent_prompt,
                "values": sorted(values, key=lambda v: v["value"]),
            })

        self._tag_specs = specs
        return specs

    def build_prompt(self, specs: list[dict] | None = None) -> str:
        """Build a classification system prompt from tag specs.

        The prompt is generated entirely from store data — no hardcoded
        tag names or values.  If a spec or value has a ``prompt`` field
        (from a ``## Prompt`` section in the tag doc), it is used as the
        classifier-facing description.
        """
        specs = specs or self._tag_specs
        if not specs:
            return ""

        sections = []
        for spec in specs:
            lines = [f"## Tag: `{spec['key']}`"]
            # Prefer prompt section over raw description
            desc = spec.get("prompt") or spec.get("description", "")
            if desc:
                lines.append(desc)
            lines.append("")
            lines.append("Values (pick at most one):")
            for v in spec["values"]:
                vdesc = v.get("prompt") or v.get("description", "")
                if vdesc:
                    lines.append(f"- `{v['value']}` — {vdesc}")
                else:
                    lines.append(f"- `{v['value']}`")
            sections.append("\n".join(lines))

        taxonomy = "\n\n".join(sections)

        # Build valid values summary for enforcement
        valid_per_key = {}
        for spec in specs:
            valid_per_key[spec["key"]] = [v["value"] for v in spec["values"]]
        valid_summary = "; ".join(
            f"{k}: {', '.join(vs)}" for k, vs in valid_per_key.items()
        )

        # Build examples from the first few tag keys
        keys = [s["key"] for s in specs]
        examples = []
        if "act" in keys:
            examples.extend([
                "1: act=assertion(0.9)",
                "2: act=commitment(0.8) status=open(0.9)",
                "3: NONE",
                "4: act=request(0.7) status=open(0.8)",
                "5: act=commitment(0.9) status=fulfilled(0.8)",
                "6: act=assessment(0.8)",
            ])
        else:
            # Generic examples for other taxonomies
            k0 = keys[0] if keys else "tag"
            v0 = specs[0]["values"][0]["value"] if specs and specs[0]["values"] else "value"
            examples.extend([
                f"1: {k0}={v0}(0.9)",
                "2: NONE",
            ])

        return f"""Classify each numbered text fragment.

{taxonomy}

Output one line per fragment. Format — one or more tag=value(confidence) pairs:
NUMBER: tag1=value1(CONFIDENCE) tag2=value2(CONFIDENCE)

CONFIDENCE is 0.0 to 1.0. If no tags apply, write:
NUMBER: NONE

Examples:
{chr(10).join(examples)}

Rules:
- ONLY use these values — {valid_summary}
- Do NOT invent new values
- If a fragment is just a preamble, heading, or meta-commentary with no substantive content, output NONE
- 0.9+ = unambiguous, 0.7-0.9 = likely"""

    def classify(
        self,
        parts: list[dict],
        specs: list[dict] | None = None,
    ) -> list[dict]:
        """Classify parts and add tags above the confidence threshold.

        Args:
            parts: List of part dicts (must have "summary" key).
            specs: Tag specs (uses loaded specs if not provided).

        Returns:
            The same parts list, with "tags" dicts added/updated in place.
        """
        specs = specs or self._tag_specs
        if not parts or not specs:
            return parts

        provider = self._provider
        if provider is None:
            return parts

        if hasattr(provider, '_provider') and provider._provider is not None:
            provider = provider._provider

        system_prompt = self.build_prompt(specs)
        if not system_prompt:
            return parts

        # Build user message: numbered fragments
        fragment_lines = []
        for i, part in enumerate(parts, 1):
            summary = part.get("summary", "").strip()
            if summary:
                fragment_lines.append(f"{i}: {summary}")

        if not fragment_lines:
            return parts

        user_prompt = "Classify these fragments:\n\n" + "\n".join(fragment_lines)

        try:
            result = provider.generate(system_prompt, user_prompt, max_tokens=2048)
            if result:
                self._apply_classifications(parts, result, specs)
        except Exception as e:
            logger.warning("Tag classification failed: %s", e)

        return parts

    def _apply_classifications(
        self,
        parts: list[dict],
        llm_output: str,
        specs: list[dict] | None = None,
    ) -> None:
        """Parse LLM output and apply tags to parts above threshold.

        Validates assigned values against the taxonomy specs to reject
        invented values from small models.
        """
        # Build valid value sets for validation
        valid_values: dict[str, set[str]] = {}
        if specs:
            for spec in specs:
                valid_values[spec["key"]] = {v["value"] for v in spec["values"]}

        for line in llm_output.strip().splitlines():
            line = line.strip()
            m = _CLASSIFY_LINE_RE.match(line)
            if not m:
                continue

            part_num = int(m.group(1))
            assignments_str = m.group(2).strip()

            if assignments_str == "NONE" or not assignments_str:
                continue

            # 1-indexed → 0-indexed
            if part_num < 1 or part_num > len(parts):
                continue
            part = parts[part_num - 1]

            # Parse tag=value(confidence) assignments
            tags = part.get("tags", {})
            for tm in _TAG_ASSIGNMENT_RE.finditer(assignments_str):
                key = tm.group(1)
                value = tm.group(2)

                # Validate against taxonomy
                if key in valid_values and value not in valid_values[key]:
                    logger.debug("Rejected invented value: %s=%s", key, value)
                    continue

                try:
                    confidence = float(tm.group(3))
                except ValueError:
                    continue

                if confidence >= self._confidence_threshold:
                    tags[key] = value

            if tags:
                part["tags"] = tags


# ---------------------------------------------------------------------------
# Register with the provider registry
# ---------------------------------------------------------------------------

get_registry().register_analyzer("default", SlidingWindowAnalyzer)
get_registry().register_analyzer("sliding-window", SlidingWindowAnalyzer)
get_registry().register_analyzer("single-pass", SinglePassAnalyzer)
