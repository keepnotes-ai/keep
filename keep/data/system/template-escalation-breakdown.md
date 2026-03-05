---
tags:
  category: system
  context: template
  type: escalation-template
---
{
  "id": "esc/breakdown/{{event_id}}",
  "summary": "Processing breakdown: {{task_type}} {{item_id}}",
  "content": "Background processing failed and needs triage.\n\ntask_type: {{task_type}}\nitem_id: {{item_id}}\ncollection: {{collection}}\nattempt: {{attempt}}\nclassification: {{failure_class}}\nerror: {{error}}\n\nReview the item and choose retry, policy adjustment, or acceptance of loss.\nWhen resolved, replace tag status from open to fulfilled or withdrawn.",
  "tags": {
    "act": "request",
    "status": "open",
    "type": "breakdown",
    "topic": "processing",
    "task_type": "{{task_type}}",
    "source_id": "{{item_id}}"
  }
}
