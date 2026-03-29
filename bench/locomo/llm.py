# Copyright (c) 2026 Inguz Outcomes LLC.
"""LLM backend for LoCoMo benchmark query answering.

Only OpenAI is used in the published results (gpt-4o-mini for both
query generation and judging). Embeddings and analysis use Ollama
locally, but those are handled by keep directly.
"""

import os
import sys


class OpenAILLM:
    """OpenAI API backend."""

    def __init__(self, model_name: str = "gpt-4o-mini"):
        import openai
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            try:
                import subprocess
                api_key = subprocess.check_output(
                    ["security", "find-generic-password", "-s", "openai-api-key", "-w"],
                    stderr=subprocess.DEVNULL
                ).decode().strip()
            except (subprocess.CalledProcessError, FileNotFoundError):
                pass
        if not api_key:
            print("Set OPENAI_API_KEY or store in macOS keychain as 'openai-api-key'")
            sys.exit(1)
        self.client = openai.OpenAI(api_key=api_key)
        self.model_name = model_name

    def generate(self, prompt: str, max_tokens: int = 1024, temperature: float = None) -> str:
        kwargs = dict(
            model=self.model_name,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
        )
        if temperature is not None:
            kwargs["temperature"] = temperature
        response = self.client.chat.completions.create(**kwargs)
        return response.choices[0].message.content.strip()


def create_llm(backend: str = "openai", model: str = None) -> OpenAILLM:
    """Create LLM instance. Only OpenAI is supported."""
    if backend != "openai":
        raise ValueError(f"Unsupported backend: {backend}. Use 'openai'.")
    return OpenAILLM(model or "gpt-4o-mini")
