"""Shared Gemini client factory for Google AI Studio and Vertex AI.

Supports two authentication paths:
1. Google AI Studio: GEMINI_API_KEY or GOOGLE_API_KEY (API key)
2. Vertex AI: GOOGLE_CLOUD_PROJECT (uses Application Default Credentials)

Vertex AI is preferred when GOOGLE_CLOUD_PROJECT is set, since it bills
to the same GCP project as other infrastructure (Cloud SQL, GKE, etc.).
"""

import os


def create_gemini_client(api_key: str | None = None):
    """Create a google-genai Client for either Google AI Studio or Vertex AI.

    Args:
        api_key: Explicit API key (overrides environment variables).
                 Ignored when GOOGLE_CLOUD_PROJECT is set (Vertex AI path).

    Returns:
        google.genai.Client instance

    Raises:
        RuntimeError: If google-genai library is not installed
        ValueError: If no authentication credentials are found
    """
    try:
        from google import genai
    except ImportError:
        raise RuntimeError(
            "Gemini providers require 'google-genai' library. "
            "Install with: pip install google-genai"
        )

    # Vertex AI path: use GCP project with Application Default Credentials
    project = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if project:
        location = os.environ.get("GOOGLE_CLOUD_LOCATION", "us-east1")
        return genai.Client(vertexai=True, project=project, location=location)

    # Google AI Studio path: use API key
    key = (
        api_key
        or os.environ.get("GEMINI_API_KEY")
        or os.environ.get("GOOGLE_API_KEY")
    )
    if not key:
        raise ValueError(
            "Gemini authentication required. Set one of:\n"
            "  GEMINI_API_KEY or GOOGLE_API_KEY (Google AI Studio)\n"
            "  GOOGLE_CLOUD_PROJECT (Vertex AI with Application Default Credentials)"
        )

    return genai.Client(api_key=key)
