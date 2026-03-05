from keep.api import Keeper, _text_content_id


def test_put_inline_without_id_uses_content_addressed_id(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "Inline content for content-addressed ID behavior."
        item = kp.put(content)
        assert item.id == _text_content_id(content)
    finally:
        kp.close()


def test_put_inline_without_id_is_stable_for_same_content(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "Stable inline content."
        first = kp.put(content)
        second = kp.put(content)
        assert first.id == second.id == _text_content_id(content)
        assert second.changed is False
    finally:
        kp.close()
