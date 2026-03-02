# keep now

Get or set the current working intentions.

The nowdoc is a singleton document that tracks what you're working on right now. It has full version history, so previous intentions are always accessible.

## Usage

```bash
keep now                              # Show current intentions
keep now "Working on auth flow"       # Update intentions
keep now "Auth work" -t project=myapp # Update with tag
echo "piped content" | keep now       # Set from stdin
```

## Options

| Option | Description |
|--------|-------------|
| `--reset` | Reset to default from system |
| `-V`, `--version N` | Version selector (`N>=0` from current, `N<0` from oldest; `-1` oldest) |
| `-H`, `--history` | Expand version history in frontmatter |
| `-t`, `--tag KEY=VALUE` | Set tag (with content) or filter (without content) |
| `-n`, `--limit N` | Max similar/meta items to show (default 3) |
| `--scope SCOPE` | Scope for multi-user isolation (e.g. user ID) |
| `-s`, `--store PATH` | Override store directory |

## Tag behavior

Tags behave differently depending on mode:

- **With content:** `-t` sets tags on the update
- **Without content:** `-t` filters version history to find the most recent version with matching tags

```bash
keep now "Auth work" -t project=myapp   # Sets project=myapp tag
keep now -t project=myapp               # Finds recent version with that tag
```

## Version navigation

The nowdoc retains full history. Each update creates a new version.

```bash
keep now -V 1              # Previous intentions
keep now -V 2              # Two versions ago
keep now --history         # List all versions
```

Output includes `prev:` navigation for browsing version history. See [VERSIONING.md](VERSIONING.md) for details.

## Similar items and meta sections

When updating (`keep now "..."`), the output surfaces similar items and meta sections as occasions for reflection:

```yaml
---
id: now
tags:
  project: "myapp"
  topic: "auth"
similar:
  - %a1b2c3d4 OAuth2 token refresh pattern...
meta/todo:
  - %e5f6a7b8 validate redirect URIs
meta/learnings:
  - %c9d0e1f2 Token refresh needs clock sync
---
Working on auth flow
```

## keep move

When a string of work is complete, move the now history into a named note. Requires either `-t` (tag filter) or `--only` (cherry-pick the tip).

```bash
keep move "auth-string" -t project=myapp   # Move matching versions
keep move "quick-note" --only              # Move just the current version
```

Moving to an existing name appends incrementally. Use `--from` to reorganize between items. See [KEEP-MOVE.md](KEEP-MOVE.md) for details.

## keep prompt reflect

Deep structured reflection practice. Guides you through gathering context, examining actions, and updating intentions.

```bash
keep prompt reflect                       # Reflect on current work
keep prompt reflect "auth flow"           # Reflect with search context
keep prompt reflect --since P7D           # With time filter
```

See [KEEP-PROMPT.md](KEEP-PROMPT.md) for full prompt options including `--id`, `--tag`, `--since`, `--until`.

## See Also

- [KEEP-PROMPT.md](KEEP-PROMPT.md) — Agent prompts with context injection
- [KEEP-MOVE.md](KEEP-MOVE.md) — Move now history into named items
- [TAGGING.md](TAGGING.md) — Tag system and speech-act tracking
- [VERSIONING.md](VERSIONING.md) — Version history and navigation
- [META-TAGS.md](META-TAGS.md) — Contextual queries (`.meta/*`)
- [REFERENCE.md](REFERENCE.md) — Quick reference index
