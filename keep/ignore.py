"""Global store-level ignore patterns.

The ``.ignore`` system doc contains fnmatch glob patterns (one per line)
that are excluded from all directory walks and watches.  This module
provides parsing, merging, and matching helpers.
"""

from __future__ import annotations

import fnmatch
from pathlib import PurePosixPath
from typing import Optional


def parse_ignore_patterns(text: str) -> list[str]:
    """Parse ``.ignore`` doc content into a list of glob patterns.

    One pattern per line.  Lines starting with ``#`` are comments.
    Blank lines and leading/trailing whitespace are ignored.
    """
    patterns: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        patterns.append(stripped)
    return patterns


def merge_excludes(
    global_patterns: list[str],
    local_patterns: Optional[list[str]],
) -> list[str]:
    """Merge global ``.ignore`` patterns with per-watch/per-put excludes.

    Returns a deduplicated combined list (global first, local appended).
    """
    if not global_patterns and not local_patterns:
        return []
    seen: set[str] = set()
    result: list[str] = []
    for pat in list(global_patterns or []) + list(local_patterns or []):
        if pat not in seen:
            seen.add(pat)
            result.append(pat)
    return result


def match_file_uri(uri: str, patterns: list[str]) -> bool:
    """Test if a ``file://`` URI's path matches any ignore pattern.

    Extracts the path from the URI and tests each suffix window against
    the patterns with ``fnmatch``.  For example, ``dist/*`` matches
    ``file:///a/b/dist/bundle.js`` because the suffix ``dist/bundle.js``
    matches the pattern.

    Returns False for non-``file://`` URIs.
    """
    if not uri.startswith("file://"):
        return False
    if not patterns:
        return False

    path = uri.removeprefix("file://")
    parts = PurePosixPath(path).parts  # ('/', 'a', 'b', 'dist', 'bundle.js')

    # Test every suffix window: basename first, then progressively longer
    # e.g. for /a/b/dist/bundle.js: "bundle.js", "dist/bundle.js", "b/dist/bundle.js", ...
    for i in range(len(parts) - 1, 0, -1):
        suffix = "/".join(parts[i:])
        for pat in patterns:
            if fnmatch.fnmatch(suffix, pat):
                return True

    return False
