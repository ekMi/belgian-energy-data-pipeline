import pytest

from orchestration_decision import decide_action


def test_already_ingested():
    assert (
        decide_action(
            already_ingested=True,
            rows_changed=None,
        )
        == "NO_CHANGE"
    )


def test_new_observations():
    assert (
        decide_action(
            already_ingested=False,
            rows_changed=0,
        )
        == "INGEST"
    )


def test_historical_corrections():
    assert (
        decide_action(
            already_ingested=False,
            rows_changed=3,
        )
        == "AWAITING_APPROVAL"
    )


def test_missing_preview():
    with pytest.raises(ValueError):
        decide_action(
            already_ingested=False,
            rows_changed=None,
        )


def test_negative_count():
    with pytest.raises(ValueError):
        decide_action(
            already_ingested=False,
            rows_changed=-1,
        )


def test_already_ingested_with_differences():
    assert (
        decide_action(
            already_ingested=True,
            rows_changed=3,
        )
        == "NO_CHANGE"
    )


@pytest.mark.parametrize("changed", [0, 3])
def test_trusted_full_export_can_be_ingested(changed):
    assert (
        decide_action(
            already_ingested=False, rows_changed=changed, source_policy="elia-full"
        )
        == "INGEST"
    )


def test_partial_trusted_declaration_is_rejected():
    with pytest.raises(ValueError, match="partial"):
        decide_action(
            already_ingested=False,
            rows_changed=3,
            source_policy="elia-full",
            input_scope="partial",
        )
