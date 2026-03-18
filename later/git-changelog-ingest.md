# Git Changelog Ingest

Date: 2026-03-18
Status: Implemented (v0.103.x)

Note: Tag names use non-system prefixes (git_sha, git_author, author)
instead of _-prefixed names from original design, since user-provided
system tags are filtered by put(). The author tag uses email as the
join key per discussion.

## Problem

`keep put -r` on a git repo captures file content but ignores the
changelog. Commit messages contain the *why* behind changes — exactly
what semantic search is best at surfacing. Today, searching "why was
the auth flow changed" returns the code but not the commit that
explains the reasoning.

## Design

### Commits as items

Each commit becomes a keep item:

```
id:      git://{repo_name}#{sha_short}
summary: {commit message}
tags:
  _source: git
  _git_sha: {full_sha}
  _git_author: {author}
  _git_date: {author_date}
  references: [file:///path/to/changed_file, ...]
```

The `references` tag creates edges from the commit to every file
it touched. This makes commits discoverable via deep search from
any file.

### Files reference their last commit

When indexing files, tag each with its last commit:

```
tags:
  git_commit: git://{repo_name}#{sha_short}
```

Define `git_commit` as an edge tag (with `_inverse: git_file`).
Now:
- File → commit: "what commit last changed this file?"
- Commit → files: "what files did this commit touch?"
- Deep search from a file surfaces the commit message
- Deep search from a commit surfaces the files

### Tags and releases

Git tags become keep items:

```
id:      git://{repo_name}@{tag_name}
summary: {tag annotation or "Release {tag_name}"}
tags:
  _source: git
  _git_tag: {tag_name}
  references: [git://{repo_name}#{sha1}, git://{repo_name}#{sha2}, ...]
```

References point to commits included in the tag (since previous tag).

### Incremental ingest

Store a watermark tag on the repo's root directory item:

```
file:///path/to/repo/
  _git_watermark: {last_ingested_sha}
```

On re-scan:
1. Read watermark from directory item
2. `git log {watermark}..HEAD --format=...` for new commits
3. Ingest new commits
4. Update watermark
5. First ingest: no watermark → `git log --all`

### Trigger

Background task enqueued after `put -r` on a directory that contains
a `.git/` folder. The existing after-write flow can dispatch this.

Task type: `ingest_git_log`
Input: `{directory: "/path/to/repo", watermark: "abc123" | null}`

### Edge tag definition

New system doc `.tag/git_commit`:

```yaml
description: Git commit that last modified this file
_inverse: git_file
_constrained: false
```

### Implementation sequence

1. New action: `keep/actions/ingest_git.py`
   - Runs `git log` with appropriate format
   - Creates commit items via mutations
   - Tags files with `git_commit`
   - Updates watermark
2. Edge tag doc: `keep/data/system/tag-git_commit.md`
3. Trigger: in after-write flow or watch re-put, detect `.git/`
   and enqueue `ingest_git_log`
4. CLI: `keep put /path/to/repo -r` triggers git ingest automatically

### Scale considerations

- Large repos (10K+ commits): first ingest is slow but one-time.
  Process in batches of 100 commits per work item.
- Incremental: typically 1-50 new commits per scan. Fast.
- Diff stats only (not full diffs) — keeps items small.
- Commit messages are short — no summarization needed.

### Search examples

After ingest:

```bash
keep find "why was the auth flow changed"
# Returns: git://keep#a1b2c3d "refactor auth to use OAuth2 instead of sessions"

keep find "auth" --deep
# Returns: file:///src/auth.py
#   deep: git://keep#a1b2c3d "refactor auth to use OAuth2..."

keep get file:///src/auth.py
# tags: git_commit: git://keep#a1b2c3d
# edges: git_commit → "refactor auth to use OAuth2..."
```
