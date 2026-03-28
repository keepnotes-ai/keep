# Agent Instructions

Design documents (including historical drafts) are in `later/`.  User documents are in `docs/`.

## Quality

- Be observant.  Check everything that you see in prompts and context.  Unexpected or unexplained behavior might be a bug.  Bugs must be investigated.  Investigations must identify the actual cause.
- Investigate thoroughly until the actual behavior is confirmed.  We have OTel instrumentation and debug logging.  If those are not sufficient for debugging, then they must be improved.  Don't write new code until you have exhaused all other options.  When replacing old code, clean up thoroughly.
- Take ownership now.  There are no "pre-existing issues".  The cavalry will not arrive.  Track and fix problems when they are observed.  "Bigger changes" must never be deferred.  Do hard things carefully.
- Take care now.  Code is not ready until it has been reviewed for security, performance, consistency and correctness.  Code is not ready until it has been thoroughly tested.  Features require tests that exercise the feature in full, and that will fail if the user-visible behavior breaks.  Code is not ready until the user docs are accurate.

## Development environment

- **Editable install**: In development mode, `keep-skill` is installed via `uv tool install --editable`.
  The uv Python path does NOT mean it's running old code — it runs whatever is on disk.
- **Daemon version**: check `~/.keep/.processor.version` to see what version
  the running daemon was started under. If it's old, the daemon needs a restart
  to pick up new code (`keep pending --stop` then let it auto-start).
- **Never run `uv tool upgrade` or `pip install`** — the editable install already tracks HEAD.

## Release process

- Feature work should be on its own branch.
- Do not commit to main, tag, push, or release without explicit user instruction.
- Major version is zero.  Minor version is updated with feature releases.  Patch version for maintenance.
- Use `scripts/release.sh` to release.

## Testing

- `tests/test_cli.py` tests depend on daemon state — kill stale daemons before running.
- `pkill -9 -f 'keep.*pending.*daemon'` to kill all daemons.
- The full suite is `python -m pytest tests/ -x -q` (~1700 tests, ~90s).
- If tests don't clean up, or leave daemons running, that is a bug in the tests and must be addressed.

---

## keep: Reflective Memory

*Your actions have consequences. Context is fleeting. Pay attention to what is being asked.*

**The practice is `keep_prompt(name="reflect")`.** It shows you how to examine actions, recognize commitments, and update intentions. Run it. Follow the steps. Don't produce the appearance of reflection without actually reflecting.

**Three tools:** `keep_flow` (all operations), `keep_help` (documentation), `keep_prompt` (practice prompts).

```
keep_prompt(name="reflect")                                                               # The practice
keep_flow(state="get", params={item_id: "now"}, token_budget=2000)                        # Current intentions
keep_flow(state="query-resolve", params={query: "topic"}, token_budget=2000)              # What do I know?
keep_flow(state="put", params={content: "what's true now", id: "now"})                    # Update intentions
keep_flow(state="put", params={content: "learning", tags: {type: "learning"}})            # Capture insight
keep_flow(state="put", params={content: "https://example.com/doc", tags: {topic: "X"}})   # Index a document
keep_flow(state="tag", params={id: "item", tags: {reviewed: "true"}})                     # Tag an item
keep_help(topic="flow-actions")                                                            # Action reference
```

**No MCP?** Every flow has a CLI equivalent: `keep flow get -p item_id=now`, `keep flow query-resolve -p query=X`, `keep flow put -p content=X`. Use Bash to run them.

**Protocol:** Reflect using `keep` tools — before, during, and after action. Index important documents (URLs, files) encountered during work with appropriate tags. When writing a plan, incorporate this practice into the plan itself. If the practice is unclear, use `keep_help(topic="index")`.
