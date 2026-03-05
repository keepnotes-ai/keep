---
tags:
  category: system
  context: template
  type: escalation-template
---
{
  "id": "esc/decision/{{event_id}}",
  "summary": "Decision needed for flow {{flow_id}}",
  "content": "Background processing paused and needs a decision.\n\nflow_id: {{flow_id}}\ngoal: {{goal}}\nstage: {{stage}}\nrequested_work: {{requested_count}}\nrequested_kinds: {{requested_kinds}}\nreason: {{reason}}\n\nResume by calling continue_flow with flow_id={{flow_id}} after deciding how to proceed.\nWhen resolved, replace tag status from open to fulfilled or withdrawn.",
  "tags": {
    "act": "request",
    "status": "open",
    "type": "decision",
    "topic": "continuations",
    "flow_id": "{{flow_id}}"
  }
}
