#!/usr/bin/env python3
"""Bump version across all files that carry it.

Usage:
    python scripts/bump_version.py 0.45.0
    python scripts/bump_version.py          # show current version
"""

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Every file that carries a version string, with a regex whose group(1) is
# the prefix to preserve and whose match is the full "prefix + old version".
TARGETS = [
    ("pyproject.toml",                                 r'(version\s*=\s*)"[^"]+"'),
    ("SKILL.md",                                       r'(version:\s*)\S+'),
    ("keep/data/openclaw-plugin/openclaw.plugin.json", r'("version":\s*)"[^"]+"'),
    ("keep/data/openclaw-plugin/package.json",         r'("version":\s*)"[^"]+"'),
    ("keep/data/openclaw-plugin/src/index.ts",             r'(version:\s*)"[^"]+"'),
    ("keep/data/openclaw-plugin/src/mcp-transport.ts",   r'(version:\s*)"[^"]+"'),
    ("claude-code-plugin/.claude-plugin/plugin.json",    r'("version":\s*)"[^"]+"'),
]


def current_version() -> str:
    text = (ROOT / "pyproject.toml").read_text()
    m = re.search(r'version\s*=\s*"([^"]+)"', text)
    if not m:
        raise RuntimeError("Cannot find version in pyproject.toml")
    return m.group(1)


def bump_file(path: Path, pattern: str, new_version: str) -> None:
    text = path.read_text()
    if '"' in pattern:
        repl = rf'\g<1>"{new_version}"'
    else:
        repl = rf"\g<1>{new_version}"
    new_text, n = re.subn(pattern, repl, text, count=1)
    if n == 0:
        raise RuntimeError(f"Pattern not found in {path}")
    path.write_text(new_text)


def main() -> None:
    old = current_version()

    if len(sys.argv) < 2:
        print(f"Current version: {old}")
        print(f"\nUsage: python scripts/bump_version.py <new-version>")
        return

    new = sys.argv[1]
    if new == old:
        print(f"Already at {old}")
        return

    for relpath, pattern in TARGETS:
        path = ROOT / relpath
        bump_file(path, pattern, new)
        print(f"  {relpath}: {old} -> {new}")

    print(f"\nBumped {len(TARGETS)} files to {new}")
    print(f"(keep/__init__.py reads from package metadata — no edit needed)")

    # Re-resolve the lock file so dependency versions stay current.
    print("\nRunning uv lock ...")
    subprocess.run(["uv", "lock"], cwd=ROOT, check=True)

    # Rebuild the OpenClaw plugin so dist/index.js matches the new version.
    plugin_dir = ROOT / "keep" / "data" / "openclaw-plugin"
    print("\nRebuilding OpenClaw plugin ...")
    subprocess.run(["node", "build.mjs"], cwd=plugin_dir, check=True)


if __name__ == "__main__":
    main()
