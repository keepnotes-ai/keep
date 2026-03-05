---
tags:
  category: system
  context: tag-description
  _constrained: "true"
  _singular: "true"
---
# Tag: `act` — Speech-Act Category

The `act` tag classifies an item by what kind of speech act it performs, grounded in Searle's taxonomy and the Language-Action Perspective (Winograd & Flores).

Tagging items by speech act makes the structure of work visible: what has been promised, what has been requested, what is being offered, what has been asserted.

## Values

| Value | Searle Category | What it marks | Example |
|-------|----------------|---------------|---------|
| `commitment` | Commissive | A promise or pledge to act | "I'll fix auth by Friday" |
| `request` | Directive | Asking someone to do something | "Please review the PR" |
| `offer` | Pre-commissive | Proposing to do something (not yet committed) | "I could refactor the cache layer" |
| `assertion` | Assertive | A claim of fact | "The tests pass on main" |
| `assessment` | Evaluative | A judgment or evaluation | "This approach is risky" |
| `declaration` | Declarative | Changing reality by utterance | "Released v0.23.0" |

## Lifecycle pairing

Three act values represent open-ended speech acts that have a lifecycle: `commitment`, `request`, and `offer`. These pair naturally with the `status` tag to track state (`open`, `fulfilled`, `declined`, `withdrawn`, `renegotiated`). See `keep get .tag/status` for details.

The other three — `assertion`, `assessment`, `declaration` — are typically complete at the moment of utterance and don't need lifecycle tracking.

## Relationship to `type`

The `act` tag is orthogonal to `type`. An item can be both `type=learning` and `act=assessment` — the learning is *about* an assessment. Or `type=breakdown` and `act=commitment` — the breakdown occurred in a commitment.

## Prompt

Classify the speech act being performed — what the speaker is *doing* with their words. When a summary describes what someone said or did ("The user requested...", "The assistant recommended..."), classify the described speech act, not the act of summarizing.

Key distinctions:
- assertion vs assessment: assertion states facts ("tests pass"); assessment judges quality ("this approach is risky")
- assertion vs request: stating information is assertion; asking someone to act is request
- commitment vs offer: commitment binds the speaker ("I will do X"); offer proposes without binding ("I could do X")
- commitment vs declaration: commitment is about future action; declaration changes state now ("released v2.0")
- request vs assertion: "You should use X" giving advice is assertion; "Please do X" asking for action is request

Only assign status when act is commitment, request, or offer. Assertions, assessments, and declarations do not have lifecycle status.

## Injection

This document feeds into analysis in two ways:

1. **Guide context** — when `analyze --tags act` is used, the full text of this doc is prepended to the analysis prompt as context for decomposition.
2. **Classification** — because `_constrained: true`, the `## Prompt` sections (here and in value sub-docs like `.tag/act/commitment`) are assembled by `TagClassifier` into a classification system prompt. Each analyzed part is classified against this taxonomy and tagged if confidence exceeds the threshold.

To customize classification behavior, edit the `## Prompt` section. The classifier only sees `## Prompt` content, not the full doc.

## Examples

```bash
# Track a commitment with lifecycle
keep put "I'll review the PR tomorrow" -t act=commitment -t status=open -t topic=code-review

# Record a request
keep put "Please add error handling to the API endpoints" -t act=request -t status=open -t project=myapp

# Capture an offer
keep put "I could refactor the cache layer if that would help" -t act=offer -t status=open

# State a fact
keep put "The CI pipeline passes on main as of today" -t act=assertion

# Make an evaluation
keep put "The current auth approach won't scale past 10k users" -t act=assessment -t topic=auth

# Declare a change
keep put "Released v2.0 — new API is live" -t act=declaration -t project=myapp

# Query open commitments
keep list -t act=commitment -t status=open

# Query all requests, any status
keep list -t act=request

# Find commitments in a project
keep find "auth" -t act=commitment -t project=myapp
```
