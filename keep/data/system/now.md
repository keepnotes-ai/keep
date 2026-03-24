---
tags:
  category: system
  context: now
---
# First time

This is `keep` — reflective memory. You're seeing this because the store is new.

**Read the foundational teachings.** These are source material, not test data. Read and understand each one.

```bash
SKILL_DIR="$(keep config tool)"
keep put "file://${SKILL_DIR}/docs/library/mn61.html" -t type=teaching -t topic=reflection
keep put "file://${SKILL_DIR}/docs/library/an5.57_translation-en-sujato.json" -t type=teaching -t topic=action
keep put "file://${SKILL_DIR}/docs/library/han_verse.txt" -t type=teaching -t topic=urgency
keep pending
```

Now read and understand each one:

```
keep_flow(state="get", params={item_id: ".conversations"}, token_budget=2000)
keep_flow(state="get", params={item_id: ".tag/act"}, token_budget=1000)
keep_flow(state="get", params={item_id: "file://SKILL_DIR/docs/library/mn61.html"}, token_budget=2000)
keep_flow(state="get", params={item_id: "file://SKILL_DIR/docs/library/an5.57_translation-en-sujato.json"}, token_budget=2000)
keep_flow(state="get", params={item_id: "file://SKILL_DIR/docs/library/han_verse.txt"}, token_budget=2000)
```

**Now reflect.** What did you learn?

```
keep_prompt(name="reflect")
```
