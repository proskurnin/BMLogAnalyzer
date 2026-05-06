from dataclasses import replace
from datetime import datetime

from analytics.oda_cda import oda_cda_repeat_rows, oda_cda_repeat_summary_rows
from tests.test_counters import make_event


def test_oda_cda_repeat_rows_track_repeat_success_after_marker_failure():
    failure = replace(
        make_event(14, message="ODA failed"),
        source_file="bm/a.log",
        line_number=10,
        timestamp=datetime(2026, 5, 1, 10, 0, 0),
        raw_line="PaymentStart resp: {Code:14 Message:ODA failed}",
    )
    repeat = replace(
        make_event(0, message="OK"),
        source_file="bm/a.log",
        line_number=11,
        timestamp=datetime(2026, 5, 1, 10, 0, 2),
        raw_line="PaymentStart resp: {Code:0 Message:OK}",
    )

    rows = oda_cda_repeat_rows([failure, repeat])
    summary = {row["metric"]: row for row in oda_cda_repeat_summary_rows(rows)}

    assert len(rows) == 1
    assert rows[0]["markers"] == "ODA"
    assert rows[0]["repeat_outcome"] == "repeat_success"
    assert summary["oda_cda_or_basic_check_events"]["value"] == 1
    assert summary["outcome_repeat_success"]["value"] == 1
    assert summary["marker_ODA"]["value"] == 1


def test_oda_cda_summary_states_when_markers_are_absent():
    rows = oda_cda_repeat_rows([make_event(3, message="Ошибка чтения карты")])
    summary = {row["metric"]: row for row in oda_cda_repeat_summary_rows(rows)}

    assert summary["oda_cda_or_basic_check_events"]["value"] == 0
    assert "not found" in summary["fact_from_logs"]["message"]
