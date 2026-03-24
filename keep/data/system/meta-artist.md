---
tags:
  category: system
  context: meta
---
# .meta/artist — Same Artist
#
# Groups media items by artist. Only activates when the viewed item
# has an `artist` tag. See .meta/genre for how the `when` guard works.
match: sequence
rules:
  - when: "!(has(params.artist) && params.artist != '')"
    return: done
  - id: same_artist
    do: find
    with:
      similar_to: "{params.item_id}"
      tags: {artist: "{params.artist}"}
      limit: "{params.limit}"
  - return:
      status: done
      with:
        same_artist: "{same_artist}"
