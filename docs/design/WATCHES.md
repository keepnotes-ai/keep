# Watches — daemon-driven source monitoring

**Status:** design draft
**Date:** 2026-03-17

## Summary

A `.watches` system doc that tracks files, directories, and URLs for
automatic re-import when their content changes. The daemon polls watched
sources on its tick and re-puts anything that has changed.

## Motivation

Users `keep put` files and URLs, but the store goes stale when sources
change. Today the only option is to re-run `put` manually. Watches make
the store a live mirror of sources the user cares about.

## Design

### System doc hierarchy

```
.watches            → default interval (30s), lists all active watches
.watches/PT5M       → entries checked every 5 minutes
.watches/P7D        → entries checked weekly
```

The base `.watches` doc uses a 30-second default poll interval.
Override docs use ISO 8601 duration suffixes to set a different interval
for their entries. This follows the existing system doc hierarchy pattern.

### Watch entry schema

Each entry in a watches doc:

| Field         | Type   | Description |
|---------------|--------|-------------|
| `uri`         | string | Source URI (`file://`, `https://`, directory path) |
| `recurse`     | bool   | Recurse into subdirectories (directories only) |
| `exclude`     | list   | fnmatch glob patterns to skip (directories only) |
| `last_check`  | float  | Unix timestamp of last poll |
| `last_hash`   | string | Content hash or directory walk-hash from last check |
| `last_mtime`  | float  | mtime from last check (files only) |
| `last_etag`   | string | HTTP ETag from last check (URLs only) |

### Detection strategy by source type

**Files:** compare mtime → if changed, re-hash content → re-put if hash differs.

**Directories:** walk with same logic as `_list_directory_files` (respects
`.gitignore`, hidden files, `--exclude` patterns) → hash the sorted
(relpath, mtime) tuples → if walk-hash differs, diff against known
`file://` items in the store and re-put changed/new files.

**URLs:** HTTP conditional GET with `If-None-Match` (ETag) /
`If-Modified-Since` → re-put on 200, skip on 304.

### CLI surface

```bash
keep put ./notes/ -r --watch              # import + start watching
keep put ./notes/ -r --watch -x "*.log"   # with exclude patterns
keep put https://example.com/doc --watch  # watch a URL
keep put ./notes/ --unwatch               # stop watching
keep pending                              # shows active watches
```

`--watch` on `keep put`:
1. Performs the normal put/directory-import.
2. Adds an entry to `.watches` (or the appropriate interval override doc).

`--unwatch` on `keep put`:
1. Removes the matching entry from `.watches`.
2. Does NOT remove already-imported items from the store.

### Daemon behavior

**Poll loop:** on each tick (30s default), the daemon:
1. Loads `.watches` and all `.watches/*` override docs.
2. For each entry whose `last_check + interval` has elapsed:
   - Check the source for changes (mtime/hash/etag).
   - If changed → re-put, update `last_check`/`last_hash`/etc.
   - If source gone → tag entry `stale: true`, surface in `keep pending`.
3. Persist updated check timestamps back to the watches doc.

**Daemon lifetime:** active watches count as pending work. The daemon
stays alive as long as any watch entries exist, even if the task queue is
empty. Removing all watches lets the daemon drain and exit normally.

**Tick cadence:** when the only remaining work is watch-checks (no
embedding/summarization queue), the daemon can sleep for the shortest
upcoming watch interval rather than its normal processing tick.

### Pending integration

`keep pending` should show active watches:

```
watching:
  ./notes/          (30s, last check 2s ago, 12 files)
  https://ex.com/x  (5m, last check 3m ago)
  ./todo.md         (30s, stale — source missing)
```

### Deleted sources

If a watched source disappears (file deleted, URL 404), the watch entry
is tagged `stale: true`. It appears in `keep pending` as a warning.
Already-imported items remain in the store. The user can `--unwatch` to
acknowledge and remove the stale entry.

## Known concurrency note

If a user manually runs `keep put ./file --watch` while the daemon is
mid-poll, both could call `put()` on the same file concurrently. Keep's
SQLite WAL handles data integrity, so this is safe but may produce
double logging. Acceptable in practice.

## Open questions

- Should `--watch` on a directory store exclude patterns from the original
  `--exclude` flags, or should the user re-specify them?
  **Proposal:** capture them at watch-creation time.
- Should there be a max-watches limit to prevent accidental
  `keep put / -r --watch`?
  **Proposal:** yes, default 100, configurable in `keep.toml`.
- Should URL watches respect `Cache-Control` headers for interval hints?
  **Proposal:** not in v1, but worth considering.
