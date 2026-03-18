# .ignore — Global Store Ignore List

Date: 2026-03-18
Status: Implemented (v0.103.1)

## Problem

Watches index everything in a directory that passes `.gitignore` and
user `--exclude` patterns. But many files shouldn't be in the store
at all — build artifacts, minified JS, lock files, generated HTML.
Today there's no global exclusion mechanism and no way to retroactively
remove items that shouldn't have been indexed.

## Design

A `.ignore` system doc in the store, shipped with sensible defaults.
Consulted by all ingestion paths. Editable by users.

### Format

```
# .ignore — glob patterns, one per line (# comments, blank lines ok)
# Applied to the relative path within a directory, or the full URI

# Build artifacts
*.min.js
*.min.css
*.bundle.js
*.map

# Package managers
package-lock.json
yarn.lock
pnpm-lock.yaml

# Generated
*.pyc
__pycache__/*

# Large binaries unlikely to have useful text
*.wasm
*.so
*.dylib
```

### Where it's consulted

1. **Directory put** — `_list_directory_files()` applies `.ignore`
   patterns after `.gitignore` and before user `--exclude`
2. **Watch re-put** — daemon applies `.ignore` during walk
3. **Retroactive purge** — on `.ignore` update, scan store for
   matching items and delete them + cancel pending work

### Retroactive purge

When `.ignore` is updated (via `keep put .ignore` or `keep tag`):
1. Load all patterns from the updated doc
2. Scan file:// URIs in the store matching any new pattern
3. Delete matched items (and their parts/versions)
4. Cancel any pending work items for those IDs

This could be a post-write hook on `.ignore` or a CLI command
(`keep ignore --purge`).

### Precedence

```
.gitignore          → git-tracked files only (existing)
.ignore             → global store-level ignore (new)
--exclude patterns  → per-watch/per-put overrides (existing)
```

### Bundled defaults

Ship with patterns for:
- Minified/bundled JS/CSS (*.min.js, *.bundle.js, *.min.css, *.map)
- Package lock files (package-lock.json, yarn.lock, pnpm-lock.yaml, Cargo.lock)
- Python bytecode (__pycache__/*, *.pyc)
- Build output (dist/*, build/*, .next/*, .nuxt/*)
- Binary artifacts (*.wasm, *.so, *.dylib, *.dll)
- Large data files (*.sqlite, *.db) — but NOT continuation.db which is ours

### User editing

```bash
keep get .ignore              # View current patterns
keep put .ignore "$(cat <<'EOF'
# Add custom patterns
*.generated.ts
reports/*.csv
EOF
)"
```

Appending vs replacing TBD — could support both via `keep tag .ignore --append`.

## Implementation

1. Bundled `.ignore` doc in system docs (like `.prompt/*`)
2. `load_ignore_patterns()` → reads `.ignore`, returns compiled matchers
3. Wire into `_list_directory_files()` and watch walk
4. Post-write hook or daemon task for retroactive purge
5. CLI: `keep ignore --list`, `keep ignore --purge`
