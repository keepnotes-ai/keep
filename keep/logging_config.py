"""Logging configuration for keep.

Suppress verbose library output by default for better UX.
"""

import os
import sys
import warnings

# Set environment variables BEFORE any imports to suppress warnings early
if not os.environ.get("KEEP_VERBOSE"):
    os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
    os.environ["TRANSFORMERS_VERBOSITY"] = "error"
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
    os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"


def configure_quiet_mode(quiet: bool = True):
    """Configure logging to suppress verbose library output.
    
    This silences:
    - HuggingFace transformers progress bars
    - MLX model loading messages
    - Library warnings (deprecation, etc.)
    
    Args:
        quiet: If True, suppress verbose output. If False, show everything.
    """
    if quiet:
        # Suppress HuggingFace progress bars and warnings
        os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
        os.environ["TRANSFORMERS_VERBOSITY"] = "error"
        os.environ["TOKENIZERS_PARALLELISM"] = "false"
        
        # Suppress Python warnings (including deprecation warnings)
        warnings.filterwarnings("ignore")
        
        # Configure Python logging to be less verbose
        import logging
        logging.getLogger("transformers").setLevel(logging.ERROR)
        logging.getLogger("sentence_transformers").setLevel(logging.ERROR)
        logging.getLogger("mlx").setLevel(logging.ERROR)
        logging.getLogger("chromadb").setLevel(logging.ERROR)


def enable_debug_mode():
    """Enable debug-level logging to stderr."""
    import logging

    # Re-enable warnings
    warnings.filterwarnings("default")

    # Restore library verbosity
    os.environ.pop("HF_HUB_DISABLE_PROGRESS_BARS", None)
    os.environ.pop("TRANSFORMERS_VERBOSITY", None)

    # Configure root logger for debug output
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Add stderr handler if not already present
    if not any(isinstance(h, logging.StreamHandler) and h.stream == sys.stderr
               for h in root_logger.handlers):
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%H:%M:%S"
        ))
        root_logger.addHandler(handler)

    # Set library loggers to DEBUG
    for name in ("keep", "transformers", "sentence_transformers", "mlx", "chromadb"):
        logging.getLogger(name).setLevel(logging.DEBUG)


def configure_ops_log(store_path):
    """Configure a persistent operations log for a keep store.

    Writes to {store_path}/keep-ops.log using a rotating file handler
    (1MB max, 3 backups). Always active regardless of --verbose.
    Returns the handler so it can be removed on close().
    """
    import logging
    from logging.handlers import RotatingFileHandler
    from pathlib import Path

    log_path = Path(store_path) / "keep-ops.log"
    handler = RotatingFileHandler(
        str(log_path),
        maxBytes=1_000_000,
        backupCount=3,
    )
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))

    keep_logger = logging.getLogger("keep")
    keep_logger.addHandler(handler)
    # Ensure keep logger allows INFO through even in quiet mode
    if keep_logger.level == logging.NOTSET or keep_logger.level > logging.INFO:
        keep_logger.setLevel(logging.INFO)

    return handler
