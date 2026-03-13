---
tags:
  category: system
  context: tag-description
  _constrained: "true"
  _requires: "act"
  _singular: "true"
---
# Tag: `status` — Lifecycle State

The `status` tag tracks the lifecycle state of speech acts — primarily commitments, requests, and offers. It answers: "Where does this stand?"

## Speech-act lifecycle values

| Value | Meaning | Typical transition from |
|-------|---------|------------------------|
| `open` | Active and unfulfilled | (initial state) |
| `blocked` | Cannot proceed until something else is resolved | `open` |
| `fulfilled` | Completed and satisfied | `open` |
| `declined` | Not accepted | `open` |
| `withdrawn` | Cancelled by the originator | `open` |
| `renegotiated` | Terms changed, new commitment replaces this one | `open` |

The normal flow: `open` → `fulfilled`. `blocked` is a sub-state of open — the commitment still exists but progress is gated on an external dependency. The other transitions handle exceptions — declining, withdrawing, or renegotiating.

## Adding custom status values

The `status` tag is constrained — only values with a sub-doc at `.tag/status/VALUE` are accepted. To add domain-specific values (e.g. `working`, `broken`, `needs_review`), create a sub-doc:

```bash
keep put "Active work in progress on this item." --id .tag/status/working
```

The sub-doc's existence makes the value valid. Its content serves as documentation.

## Prompt

Only assign status when act is commitment, request, or offer. Never assign status to assertions, assessments, or declarations.

YES: act=commitment status=open, act=request status=fulfilled, act=offer status=declined
NO: act=assessment status=open, act=assertion status=open, act=declaration status=fulfilled

When classifying analyzed fragments: look at later fragments to determine if a commitment/request was fulfilled, declined, or withdrawn. If the outcome is not visible in the fragments, assign status=open.

## Injection

This document feeds into analysis in two ways:

1. **Guide context** — when `analyze --tags status` is used, the full text is prepended to the analysis prompt.
2. **Classification** — because `_constrained: true`, the `## Prompt` sections (here and in value sub-docs like `.tag/status/open`) are assembled into a classification prompt by `TagClassifier`.

To customize classification behavior, edit the `## Prompt` sections. The classifier only sees `## Prompt` content, not the full doc.

## Examples

```bash
# Create an open commitment
keep put "I'll fix the auth bug by end of week" -t act=commitment -t status=open -t project=myapp

# Mark it fulfilled
keep tag ID --tag status=fulfilled

# Query open items
keep list -t act=commitment -t status=open
keep list -t act=request -t status=open

# Track a request lifecycle
keep put "Please review the PR" -t act=request -t status=open
# Later, when reviewed:
keep tag ID --tag status=fulfilled

# Withdraw an offer
keep tag ID --tag status=withdrawn

# Renegotiate a commitment (mark old, create new)
keep tag ID --tag status=renegotiated
keep put "Revised: will fix auth bug next sprint instead" -t act=commitment -t status=open
```
