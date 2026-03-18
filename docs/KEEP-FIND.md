# keep find

Find items by semantic similarity or full-text search.

## Usage

```bash
keep find "authentication"            # Semantic similarity search
keep find "auth" --text               # Full-text search on summaries
keep find --id ID                     # Find items similar to an existing item
```

## Options

| Option | Description |
|--------|-------------|
| `--text` | Use full-text search instead of semantic similarity |
| `--id ID` | Find items similar to this ID (instead of text query) |
| `--include-self` | Include the queried item (only with `--id`) |
| `-t`, `--tag KEY=VALUE` | Filter by tag (repeatable, AND logic) |
| `-n`, `--limit N` | Maximum results (default 10) |
| `--since DURATION` | Only items updated since (see time filtering below) |
| `--until DURATION` | Only items updated before (see time filtering below) |
| `-D`, `--deep` | Follow tags/edges from results to discover related items |
| `-S`, `--scope GLOB` | Constrain results to IDs matching glob pattern |
| `--tags` | Show non-system tags for each result |
| `-H`, `--history` | Include archived versions of matching items |
| `--tokens N` | Token budget for rich context output (includes parts and versions) |
| `-a`, `--all` | Include hidden system notes (IDs starting with `.`) |
| `-s`, `--store PATH` | Override store directory |

## Semantic vs full-text search

**Semantic search** (default) finds items by meaning using embeddings. "authentication" matches items about "login", "OAuth", "credentials" even if they don't contain the word "authentication".

**Full-text search** (`--text`) matches exact words in summaries. Faster but literal.

## Similar-to-item search

Find items similar to an existing document:

```bash
keep find --id file:///path/to/doc.md           # Similar to this document
keep find --id %a1b2c3d4                        # Similar to this item
keep find --id %a1b2c3d4 --since P30D           # Similar items from last 30 days
```

## Tag filtering

Combine semantic search with tag filters (AND logic):

```bash
keep find "auth" -t project=myapp               # Search within a project
keep find "auth" -t project -t topic=security    # Multiple tags (AND)
```

Tag filters are applied as **pre-filters on the vector search** — results are the best matches *within* the filtered set, not global results filtered afterward. This makes tags suitable for data isolation (per-user, per-project, etc.). See [TAGGING.md](TAGGING.md#tag-based-isolation).

## Time filtering

The `--since` and `--until` options accept ISO 8601 durations or dates:

```bash
keep find "auth" --since P7D           # Last 7 days
keep find "auth" --since P1W           # Last week
keep find "auth" --since PT1H          # Last hour
keep find "auth" --since P1DT12H       # 1 day 12 hours
keep find "auth" --since 2026-01-15    # Since specific date
keep find "auth" --until 2026-02-01    # Before specific date
keep find "auth" --since P30D --until P7D  # Between 30 and 7 days ago
```

## Output format

Results are displayed as summary lines with similarity score and date:

```
%a1b2c3d4         (0.89) 2026-01-14 OAuth2 token refresh pattern...
%e5f6a7b8         (0.82) 2026-01-13 Token handling and session management...
```

Dates reflect when the item was created. Use `--full` for complete frontmatter with tags, similar items, and version navigation.

## Scoped search

Constrain results to items whose IDs match a glob pattern:

```bash
keep find "auth" --scope 'file:///Users/me/notes/*'       # Only files in notes/
keep find "auth" --scope 'file:///path/to/memory*'         # memory/ dir + MEMORY.md
keep find "auth" --scope '*myproject*'                      # IDs containing myproject
```

The search runs globally (traversing all items for semantic matching), but only items whose base ID matches the glob are returned. Deep search can follow edges through out-of-scope items, but results are still scoped.

## Deep search

Deep search (`--deep`) follows tags and edges from the primary results to discover related items that wouldn't appear in a normal search. Results are grouped under the primary item that led to them.

```bash
keep find "authentication" --deep      # Primary results + related items
keep find "auth" --deep -n 5           # Top 5 with deep groups
```

When edge tags are defined (see [EDGE-TAGS.md](EDGE-TAGS.md)), deep search follows edges to find related items via graph traversal. Without edges, it falls back to tag-based discovery — finding items that share tags with the primary results.

Deep search is incompatible with `--history`; if both are specified, `--deep` is ignored.

## Including archived versions

```bash
keep find "auth" --history             # Also search old versions of items
```

## See Also

- [KEEP-LIST.md](KEEP-LIST.md) — List and filter by tags
- [KEEP-GET.md](KEEP-GET.md) — Retrieve full item details
- [TAGGING.md](TAGGING.md) — Tag filtering patterns
- [REFERENCE.md](REFERENCE.md) — Quick reference index
