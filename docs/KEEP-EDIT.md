# keep edit

Edit a note's content in your terminal editor.

## Usage

```bash
keep edit <id>
```

Opens the current content of the note in `$EDITOR` (or `$VISUAL`, falling back to `vi`). When you save and close the editor, the note is updated if the content changed.

## Examples

```bash
keep edit .ignore                    # Edit global ignore patterns
keep edit .prompt/agent/reflect      # Edit a prompt template
keep edit now                        # Edit current intentions
keep edit %a1b2c3d4                  # Edit an inline note
EDITOR=code keep edit .ignore        # Use VS Code
```

## How it works

1. Reads the current summary/content of the note
2. Writes it to a temporary file (`.md` by default)
3. Opens the file in your editor
4. On save, compares with the original — if changed, calls `put` to update
5. The temp file is cleaned up automatically

System docs (`.ignore`, `.prompt/*`, `.state/*`) store their full content as the summary, so `keep edit` gives you the complete document.

## See Also

- [KEEP-PUT.md](KEEP-PUT.md) — Creating and updating notes
- [KEEP-GET.md](KEEP-GET.md) — Viewing notes
- [REFERENCE.md](REFERENCE.md) — Quick reference index
