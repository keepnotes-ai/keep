# keep get

Retrieve item(s) by ID.

## Usage

```bash
keep get ID                           # Current version with similar items
keep get ID1 ID2 ID3                  # Multiple items (separated by ---)
keep get ID -V 1                      # Previous version
keep get ID -V -1                     # Oldest archived version
keep get "ID@V{1}"                    # Same as -V 1 (version identifier syntax)
keep get "ID@V{-1}"                   # Same as -V -1
```

## Options

| Option | Description |
|--------|-------------|
| `-V`, `--version N` | Version selector (`N>=0` from current, `N<0` from oldest; `-1` oldest) |
| `-H`, `--history` | Expand version history in frontmatter (default 10, use `-n` to override) |
| `-S`, `--similar` | List similar items (default 10) |
| `-M`, `--meta` | List meta items |
| `-R`, `--resolve QUERY` | Inline meta query (meta-tag syntax, repeatable) |
| `-P`, `--parts` | Expand all parts in frontmatter (from `analyze`) |
| `-t`, `--tag KEY=VALUE` | Require tag (error if item doesn't match) |
| `-n`, `--limit N` | Max items for --history, --similar, --meta (default 10) |
| `-s`, `--store PATH` | Override store directory |

## Default output

Single-item commands (`get`, `now`) default to full YAML frontmatter format:

```yaml
---
id: %a1b2c3d4
tags:
  project: "myapp"
  topic: "auth"
  type: "learning"
similar:
  - %e5f6a7b8 (0.89) 2026-01-14 Related authentication...
  - %c9d0e1f2 (0.85) 2026-01-13 Token handling notes...
meta/learnings:
  - %d3e4f5a6 Token refresh needs clock sync
prev:
  - @V{1} 2026-01-14 Previous summary text...
---
Document summary here...
```

## Multiple IDs

```bash
keep get doc:1 doc:2 doc:3            # Items separated by ---
keep --ids list -n 5 | xargs keep get # Pipe from list
```

## Parts

Access structural parts produced by `keep analyze`:

```bash
keep get "ID@P{1}"                    # Part 1 of a document
keep get "ID@P{3}"                    # Part 3
keep get ID --parts                   # List all parts
```

Parts include prev/next navigation and part-specific similar items.

## Display modes

```bash
keep get ID --history                 # Frontmatter with all versions expanded
keep get ID --history -n 20           # Show 20 versions
keep get ID --parts                   # Frontmatter with all parts expanded
keep get ID --similar                 # Show similar items
keep get ID --similar -n 20           # Show 20 similar items
keep get ID --meta                    # Show meta items
keep get ID --meta -n 5              # Show 5 meta items per section
```

The `--history` and `--parts` flags expand the `prev:` and `parts:` sections of the normal frontmatter output. Use `--ids` to get flat IDs for piping: `keep --ids get ID --parts | xargs keep get`.

## Tag filtering

```bash
keep get ID -t project=myapp          # Error if item doesn't have this tag
```

## See Also

- [VERSIONING.md](VERSIONING.md) — Version identifiers and history
- [KEEP-FIND.md](KEEP-FIND.md) — Search for items by meaning
- [META-TAGS.md](META-TAGS.md) — Contextual queries (`.meta/*`)
- [REFERENCE.md](REFERENCE.md) — Quick reference index
