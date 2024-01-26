from snuba.admin.dead_letter_queue import get_dlq_topics


def test_dlq() -> None:
    assert len(get_dlq_topics()) == 5
