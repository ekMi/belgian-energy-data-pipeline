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


def test_dbt_recovery_has_priority():
    assert (
        decide_action(
            already_ingested=True,
            rows_changed=None,
            dbt_recovery_pending=True,
        )
        == "RECOVER_DBT"
    )


def test_dbt_recovery_before_new_ingestion():
    assert (
        decide_action(
            already_ingested=False,
            rows_changed=0,
            dbt_recovery_pending=True,
        )
        == "RECOVER_DBT"
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
