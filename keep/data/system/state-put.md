---
tags:
  category: system
  context: state
---
match: sequence
rules:
  - id: stored
    do: put
    with:
      content: "{params.content}"
      uri: "{params.uri}"
      id: "{params.id}"
      tags: "{params.tags}"
      summary: "{params.summary}"
  - return:
      status: done
      with:
        id: "{stored.id}"
