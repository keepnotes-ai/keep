"""Shared Ollama utilities — base URL resolution, model check, auto-pull."""

import logging
import os
import sys

import requests

logger = logging.getLogger(__name__)


def ollama_base_url(url: str | None = None) -> str:
    """Resolve and normalize an Ollama base URL.

    Checks OLLAMA_HOST env var, defaults to localhost:11434.
    Ensures http:// prefix and strips trailing slash.
    """
    if url is None:
        url = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
    if not url.startswith("http"):
        url = f"http://{url}"
    return url.rstrip("/")


def ollama_ensure_model(base_url: str, model: str) -> None:
    """Check if an Ollama model is available locally; pull it if not.

    Streams pull progress to stderr so the user sees download status.
    Raises RuntimeError if the pull fails or Ollama is unreachable.
    """
    # Normalize model name for comparison — Ollama strips :latest
    bare = model.split(":")[0] if ":" in model else model

    # Check installed models
    try:
        resp = requests.get(f"{base_url}/api/tags", timeout=5)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(
            f"Cannot reach Ollama at {base_url}. "
            "Is Ollama running? Start it with: ollama serve"
        ) from e

    installed = {m["name"] for m in resp.json().get("models", [])}
    # Ollama lists models as "name:tag" — check both exact and bare+:latest
    if model in installed or f"{model}:latest" in installed:
        return
    if bare in installed or f"{bare}:latest" in installed:
        return

    # Model not installed — pull it
    logger.info("Pulling Ollama model %s (first use)...", model)
    print(f"Pulling Ollama model '{model}' (first use)...", file=sys.stderr)

    try:
        resp = requests.post(
            f"{base_url}/api/pull",
            json={"name": model, "stream": True},
            stream=True,
            timeout=600,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to pull Ollama model '{model}': {e}") from e

    last_status = ""
    for line in resp.iter_lines():
        if not line:
            continue
        import json
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue

        status = data.get("status", "")
        total = data.get("total", 0)
        completed = data.get("completed", 0)

        if total and completed:
            pct = int(completed / total * 100)
            msg = f"\r  {status}: {pct}%"
        elif status != last_status:
            msg = f"\n  {status}"
        else:
            continue

        print(msg, end="", file=sys.stderr, flush=True)
        last_status = status

        if data.get("error"):
            print("", file=sys.stderr)
            raise RuntimeError(
                f"Ollama pull failed for '{model}': {data['error']}"
            )

    print(f"\n  Model '{model}' ready.", file=sys.stderr)
