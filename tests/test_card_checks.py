from analytics.card_checks import card_check_marker_rows, card_check_marker_summary_rows
from tests.test_counters import make_event


def test_card_check_marker_rows_find_explicit_oda_cda_and_basic_check_markers():
    events = [
        make_event(14, message="ODA failed"),
        make_event(255, message="CDA error"),
        make_event(3, message="basic check failed"),
        make_event(0, message="OK"),
    ]

    rows = card_check_marker_rows(events)

    assert [row["markers"] for row in rows] == ["ODA", "CDA", "basic_check"]
    summary = {row["metric"]: row for row in card_check_marker_summary_rows(rows)}
    assert summary["explicit_card_check_marker_events"]["value"] == 3
    assert summary["marker_ODA"]["value"] == 1
    assert summary["marker_CDA"]["value"] == 1
    assert summary["marker_basic_check"]["value"] == 1


def test_card_check_summary_states_when_explicit_markers_are_absent():
    rows = card_check_marker_rows([make_event(14, message="Операция отклонена")])
    summary = {row["metric"]: row for row in card_check_marker_summary_rows(rows)}

    assert summary["explicit_card_check_marker_events"]["value"] == 0
    assert "not found" in summary["fact_from_logs"]["message"]
