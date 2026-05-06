from dataclasses import replace
from datetime import datetime

from analytics.card_history import timeout_card_history_rows, timeout_card_history_summary_rows
from analytics.timeouts import timeout_repeat_rows, timeout_repeat_summary_rows
from tests.test_counters import make_event


def test_timeout_repeat_rows_track_repeat_success_after_timeout():
    timeout = replace(
        make_event(16, message="Истек таймаут"),
        source_file="bm/a.log",
        line_number=10,
        timestamp=datetime(2026, 5, 1, 10, 0, 0),
        raw_line="PaymentStart resp: {Code:16 MessageRus:Истек таймаут}",
    )
    success = replace(
        make_event(0, message="OK"),
        source_file="bm/a.log",
        line_number=11,
        timestamp=datetime(2026, 5, 1, 10, 0, 2),
        raw_line="PaymentStart resp: {Code:0 MessageRus:OK}",
    )

    rows = timeout_repeat_rows([timeout, success])
    summary = {row["metric"]: row for row in timeout_repeat_summary_rows(rows)}

    assert rows[0]["repeat_outcome"] == "repeat_success"
    assert summary["timeout_events"]["value"] == 1
    assert summary["outcome_repeat_success"]["value"] == 1


def test_timeout_card_history_detects_same_card_later_success():
    timeout = replace(
        make_event(16, message="Истек таймаут"),
        source_file="bm/a.log",
        line_number=10,
        timestamp=datetime(2026, 5, 1, 10, 0, 0),
        raw_line="PaymentStart resp: {Code:16 MessageRus:Истек таймаут Bin:0 HashPan:abc VirtualCard:{VirtualUid: VirtualAppCode:0}}",
    )
    success = replace(
        make_event(0, message="OK"),
        source_file="bm/a.log",
        line_number=20,
        timestamp=datetime(2026, 5, 1, 10, 1, 0),
        raw_line="PaymentStart resp: {Code:0 MessageRus:OK Bin:0 HashPan:abc VirtualCard:{VirtualUid: VirtualAppCode:0}}",
    )

    rows = timeout_card_history_rows([timeout, success])
    summary = {row["metric"]: row for row in timeout_card_history_summary_rows(rows)}

    assert rows[0]["same_card_later_success"] == 1
    assert rows[0]["repeat_outcome"] == "no_repeat_within_3s"
    assert summary["timeout_events"]["value"] == 1
    assert summary["timeout_events_with_card_key"]["value"] == 1
    assert summary["same_card_had_later_success"]["value"] == 1
