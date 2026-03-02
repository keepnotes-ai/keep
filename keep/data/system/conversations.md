---
tags:
  category: system
  context: practice
---
# Conversation Patterns for Process Knowledge

This document describes patterns for recognizing, tracking, and learning from
the structure of work — not *what* we know, but *how work proceeds*.

These patterns are grounded in the Language-Action Perspective (Winograd & Flores),
which treats work as networks of commitments made through language.

---

## Decision Matrix: What Kind of Conversation Is This?

| Signal | Conversation Type | Your Stance | Don't Do This |
|--------|-------------------|-------------|---------------|
| "Can you...", "Please...", "I need..." | **Action** | Clarify → Promise → Deliver | Promise before understanding |
| "What if...", "Imagine...", "Could we..." | **Possibility** | Explore, generate options, hold lightly | Commit prematurely |
| "What is...", "How does...", "Why..." | **Clarification** | Explain until understanding lands | Over-answer; assume you know the real question |
| "I'm trying to understand...", "Context is..." | **Orientation** | Listen, reflect back, build shared background | Jump to solutions |
| "Here's what I found...", "Status update..." | **Report** | Acknowledge, ask what's needed next | Assume it's a request |

**Transition signals** (conversation type is shifting):
- Possibility → Action: "Let's do X" / "I want to go with..."
- Clarification → Action: "Ok, so please..." / "Now that I understand..."
- Orientation → Clarification: "So what does X mean?" / "Help me understand..."

**When uncertain:** Ask. "Are you exploring options, or would you like me to do something?"

---

## Core Insight

Work is not information processing. Work is **commitment management**.

Language is not mere description — language *is* action. When we speak, we perform acts that change the world: we request, promise, declare, assert, and evaluate. These are not decorations on top of "real" work. They are the work.

Five kinds of speech act (Searle):
- **Assertives**: Commit the speaker to a truth claim ("The tests pass")
- **Directives**: Request action from another ("Please review this")
- **Commissives**: Commit the speaker to future action — promises, pledges ("I'll fix that by Friday")
- **Declarations**: Change reality by being uttered ("This is approved", "You're hired")
- **Expressives**: Convey attitudes ("Thank you", "I'm concerned about...")

When an agent assists with a task, it participates in a conversation with structure:
- Requests create openings
- Promises create obligations
- Completion is declared, not merely achieved
- **Satisfaction** closes the loop — declared by the *customer*, not the performer

Understanding *where we are* in this structure is as important as understanding
*what we know* about the subject matter.

**Tagging convention:** Use `act` to classify the speech-act type and `status` to track lifecycle state. For full details: `keep get .tag/act` and `keep get .tag/status`.

---

## The Basic Conversation for Action

```
     Customer                          Performer
         │                                 │
         │──── 1. Request ────────────────→│
         │                                 │
         │←─── 2. Promise (or Counter) ────│
         │                                 │
         │          [ work happens ]       │
         │                                 │
         │←─── 3. Declare Complete ────────│
         │                                 │
         │──── 4. Declare Satisfied ──────→│
         │                                 │
```

At any point: Withdraw, Decline, Renegotiate.

**Tagging the conversation lifecycle:**
```bash
# At promise (step 2)
keep put "I'll implement the auth flow" -t act=commitment -t status=open -t project=myapp

# At satisfaction (step 4)
keep tag ID --tag status=fulfilled
```

**Conditions of satisfaction** are negotiated agreements — not objective goals. They are what the customer and performer agree constitutes "done." When conditions are left implicit, breakdowns follow.

**For agents:** Recognizing this structure helps answer:
- "What has been asked of me?"
- "What have I committed to?"
- "What conditions of satisfaction were established?"
- "Who declares satisfaction — and have they?"

---

## Conversation Types

### Request for Action
Someone asks for something to be done.

**Recognize by:** Imperative language, "can you", "please", "I need"

**Track:**
- What specifically was requested?
- Any conditions or constraints?
- Implicit quality criteria?
- Who is the customer (who declares satisfaction)?

**Completion:** Customer declares satisfaction, not performer declares done.

```bash
keep put "Please add pagination to the API" -t act=request -t status=open -t project=myapp
```

### Request for Information
Someone asks to know something.

**Recognize by:** Questions, "what is", "how does", "why"

**Track:**
- What gap in understanding prompted this?
- What level of detail is appropriate?
- What would make this answer useful?

**Completion:** Questioner indicates understanding (often implicit).

### Offer
Someone proposes to do something.

**Recognize by:** "I could", "would you like me to", "I suggest"

**Track:**
- What conditions on acceptance?
- What's the scope of the offer?

**Completion:** Offer accepted → becomes promise. Offer declined → closed.

```bash
keep put "I could refactor the cache layer" -t act=offer -t status=open -t topic=performance
```

### Report / Declaration
Someone asserts a state of affairs.

**Recognize by:** Statements of fact, status updates, "I found that"

**Track:**
- What changed as a result of this declaration?
- Who needs to know?

**Completion:** Acknowledgment (may trigger new conversations).

```bash
keep put "Released v2.0 — new API is live" -t act=declaration -t project=myapp
```

---

## Conversations for Possibility

Possibility conversations explore "what could be" — no commitment yet. The mood is speculative.

**Recognize by:** "What if", "imagine", "we could", "brainstorm", "options"

**Your stance:**
- Generate, don't filter prematurely
- Hold ideas lightly — nothing is promised
- Expand the space before narrowing
- Name options without advocating

**Track:**
- Options generated
- Constraints surfaced
- Energy/interest signals ("that's interesting" vs "hmm")

**Completion:** Not satisfaction — rather, either:
- Transition to Action ("let's do X")
- Explicit close ("good to know our options")
- Energy dissipates naturally

A possibility conversation should **fail** if no actionable commitments eventually emerge from it. If it was worth exploring, it should produce either a decision to act or an explicit decision not to.

**Critical:** Don't promise during possibility. "I could do X" is an option, not a commitment. The transition to action must be explicit.

```bash
# Index possibility exploration
keep put "Explored three auth options: OAuth2, API keys, magic links. \
User showed interest in magic links for UX simplicity. No decision yet." \
  --tag type=possibility --tag topic=authentication --tag status=open
```

---

## Conversations for Orientation

Orientation conversations establish shared background — the context needed for future effective communication. This includes knowledge, relationships, and attitudes.

**Recognize by:** "I'm trying to understand...", "Context is...", "Let me explain how we...", informal relationship-building

**Your stance:**
- Listen, reflect back, surface assumptions
- Build shared understanding — don't jump to solutions
- Acknowledge the relational dimension — trust is built here

**Track:**
- What shared context was established?
- What assumptions were surfaced?
- What remains unclear?

**Completion:** Not explicit — orientation is ongoing. It succeeds when future conversations proceed more smoothly because of it.

Formal onboarding and informal "shooting the bull" both serve this function. Don't underestimate the latter.

---

## Breakdowns: Where Learning Happens

A **breakdown** occurs when the normal flow is interrupted:
- Expected response doesn't come
- Declared completion isn't satisfactory
- Preconditions weren't met
- Ambiguity surfaces mid-work

**Breakdowns are valuable.** They reveal assumptions that were invisible.

**Pattern for breakdown learning:**
```
1. Notice: "This isn't going as expected"
2. Articulate: "The assumption was X, but actually Y"
3. Repair: Complete the immediate conversation
4. Record: Capture the learning for future conversations of this type
```

When indexing a breakdown:
```bash
keep put "Assumption: user wanted full rewrite. Actually: wanted minimal patch." \
  --tag type=breakdown --tag conversation_type=code_change_request
```

---

## Moods

A person's mood is driven by their vision of the future. Moods are not incidental to work — they shape what actions seem possible and what commitments feel achievable.

**Moods that serve:** Ambition, acceptance, serenity, respect, pride, camaraderie.

**Moods that undermine:** Anxiety, resentment, resignation, cynicism.

To manage a mood, create a different understanding about the future. A team stuck in resignation needs a credible possibility to shift toward. A person in anxiety needs clarity about what is actually at stake.

People who fail to manage their moods exhibit a lack of respect for their teammates. This is not about suppressing feelings — it is about taking responsibility for the atmosphere in which commitments are made.

---

## Assessments and Assertions

**Assertions** are claims of fact: "The build is broken." "This endpoint returns 404." They commit the speaker to truth.

**Assessments** are evaluations: "This code is well-structured." "The approach is risky." "We're making good progress." They express the speaker's judgment, not objective truth.

Distinguishing the two is a core competency. Ungrounded assessments — evaluations presented as facts, or evaluations without observable evidence — distort action and erode trust.

When reflecting, ask: "Am I stating a fact or making an evaluation? If an evaluation, what evidence grounds it?"

```bash
# Record an assertion (fact claim)
keep put "The CI pipeline passes on main" -t act=assertion -t topic=ci

# Record an assessment (evaluation)
keep put "The current auth approach won't scale past 10k users" -t act=assessment -t topic=auth
```

Assessments are tied to moods. A mood of resignation produces assessments like "this will never work." The assessment *feels* like a fact, but it is not. Recognizing it as an assessment opens the possibility of a different future.

---

## Trust

Trust is built and destroyed through commitments.

**Both unfulfilled promises and unnecessary requests destroy trust.** A promise not kept — even a small one — signals unreliability. A request that wastes another's time signals disrespect.

Trust is cultivated through consistent accountability conversations: making clear requests, negotiating honestly, keeping promises, declaring completion transparently, and accepting satisfaction gracefully.

When trust is damaged, repair requires: acknowledging the specific breakdown, understanding its impact, and making a new commitment that is then kept.

---

## Nested and Linked Conversations

Real work involves conversations within conversations:

```
User requests feature
  └─ Agent promises feature
       └─ Agent requests clarification (sub-conversation)
       │    └─ User provides clarification
       │    └─ Agent acknowledges (closes sub-conversation)
       └─ Agent declares complete
  └─ User requests revision (linked follow-on)
       └─ ...
```

**Track parent/child relationships** to understand scope:
- Completing a sub-conversation doesn't complete the parent
- Breakdowns in child conversations may propagate up
- Context flows down (child inherits parent context)

---

## Recurrent Patterns by Domain

### Software Development

| Pattern | Structure | Completion Condition |
|---------|-----------|---------------------|
| Bug report | Request → Diagnosis → Fix → Verify | User confirms fix works |
| Feature request | Request → Clarify → Implement → Review → Accept | Stakeholder accepts |
| Code review | Offer(changes) → Review → Request(revisions) → Update → Approve | Reviewer approves |
| Refactor | Offer → Scope agreement → Execute → Verify behavior preserved | Tests pass, reviewer approves |

### Research / Analysis

| Pattern | Structure | Completion Condition |
|---------|-----------|---------------------|
| Question | Request(info) → Search → Synthesize → Present | Questioner satisfied |
| Hypothesis test | Declare(hypothesis) → Design test → Execute → Report | Evidence evaluated |
| Literature review | Request → Gather → Synthesize → Summarize | Comprehensive & relevant |

### Planning / Coordination

| Pattern | Structure | Completion Condition |
|---------|-----------|---------------------|
| Task breakdown | Request(outcome) → Decompose → Commit to parts → Track → Integrate | Outcome achieved |
| Decision | Present options → Evaluate → Declare choice | Commitment to proceed |
| Handoff | Declare(status) → Transfer commitments → Acknowledge | Receiving agent accepts |

---

## Agent Handoff as Commitment Transfer

When one agent hands off to another:

```bash
# Outgoing agent records state
keep now "User requested OAuth2 implementation. I promised and partially delivered. \
Token acquisition works. Refresh flow incomplete. User awaiting completion." \
  --tag topic=authentication --tag state=handoff --tag act=commitment --tag status=open
```

Incoming agent reads this and knows:
- There's an open promise to the user
- What "done" looks like
- Where to pick up

```bash
# Incoming agent checks context
keep now
```

---

## Learning New Patterns

Agents can recognize and record new conversation patterns:

```bash
keep put "Pattern: Incremental Specification. \
When requirements are vague, don't promise immediately. \
Propose interpretation → get correction → repeat until clear. \
Only then commit to action. Breakdown risk: Promising too early leads to rework." \
  --tag type=conversation_pattern --tag domain=general
```

---

## Using Patterns in Practice

**At task start:**
1. What kind of conversation is this?
2. What's my role (customer or performer)?
3. What does completion look like?
4. Have I seen breakdowns in this pattern before?
5. Are there open commitments or requests? `keep list -t act=commitment -t status=open`

**Mid-task:**
1. Where are we in the conversation structure?
2. Have any sub-conversations opened?
3. Are there signs of breakdown?

**At task end:**
1. Was satisfaction declared (not just completion)?
2. Any learnings to record?
3. Open commitments to hand off?

---

## References

- Winograd, T. & Flores, F. (1986). *Understanding Computers and Cognition*
- Flores, F. (2013). *Conversations for Action and Collected Essays*
- Searle, J. (1969). *Speech Acts*
- Haeckel, S. (1999). *Adaptive Enterprise* (Commitment Management Protocol)
- Denning, P. & Medina-Mora, R. (1995). "Completing the Loops"
- Flores, F. et al. (1988). "Computer Systems and the Design of Organizational Interaction"
- Dubberly, H. "The Language/Action Model of Conversation" — https://www.dubberly.com/articles/language-action-model.html
- Ing, D. "Conversations for Action, Commitment Management Protocol" — https://coevolving.com/blogs/index.php/archive/conversations-for-action-commitment-management-protocol/
