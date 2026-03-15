---
tags:
  category: system
  context: state
---
match: sequence
rules:
  - id: profile
    do: stats
    with:
      top_k: "{params.top_k}"
  - return:
      status: done
      with:
        total: "{profile.total}"
        tags: "{profile.tags}"
        all_tags: "{profile.all_tags}"
        dates: "{profile.dates}"
        structure: "{profile.structure}"
