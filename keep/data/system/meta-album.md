---
tags:
  category: system
  context: meta
---
# .meta/album — Same Album
#
# Groups tracks from the same release. Only activates when the viewed
# item has an `album` tag. See .meta/genre for how the `when` guard works.
match: sequence
rules:
  - when: "!(has(params.album) && params.album != '')"
    return: done
  - id: same_album
    do: find
    with:
      similar_to: "{params.item_id}"
      tags: {album: "{params.album}"}
      limit: "{params.limit}"
  - return:
      status: done
      with:
        same_album: "{same_album}"
