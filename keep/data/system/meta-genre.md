---
tags:
  category: system
  context: meta
---
# .meta/genre — Same Genre
#
# Groups media items by genre. Only activates when the viewed item
# has a `genre` tag — the `when` guard checks this and returns
# early if absent.
#
# The `when` predicate replaces the old `genre=*` prerequisite
# syntax. Use `has(params.genre)` to check key existence, since
# CEL raises an error for missing keys. The guard fires (returns
# done) when the viewed item has no genre tag.
#
# Tags like `genre` are passed as params by resolve_meta():
#   params.genre = current_item.tags.get("genre", "")
match: sequence
rules:
  - when: "!(has(params.genre) && params.genre != '')"
    return: done
  - id: same_genre
    do: find
    with:
      similar_to: "{params.item_id}"
      tags: {genre: "{params.genre}"}
      limit: "{params.limit}"
  - return:
      status: done
      with:
        same_genre: "{same_genre}"
