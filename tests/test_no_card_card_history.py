from dataclasses import replace
from datetime import datetime

from analytics.card_history import no_card_card_history_rows, no_card_card_history_summary_rows
from tests.test_counters import make_event


def test_no_card_card_history_detects_repeat_success_for_same_card_key():
    no_card = replace(
        make_event(17, message="Нет карты"),
        source_file="bm/a.log",
        line_number=10,
        timestamp=datetime(2026, 5, 1, 10, 0, 0),
        raw_line="PaymentStart resp: {Code:17 MessageRus:Нет карты Bin:0 HashPan:abc VirtualCard:{VirtualUid: VirtualAppCode:0}}",
    )
    success = replace(
        make_event(0, message="OK"),
        source_file="bm/a.log",
        line_number=11,
        timestamp=datetime(2026, 5, 1, 10, 0, 2),
        raw_line="PaymentStart resp: {Code:0 MessageRus:OK Bin:0 HashPan:abc VirtualCard:{VirtualUid: VirtualAppCode:0}}",
    )

    rows = no_card_card_history_rows([no_card, success])
    summary = {row["metric"]: row for row in no_card_card_history_summary_rows(rows)}

    assert rows[0]["same_card_later_success"] == 1
    assert rows[0]["repeat_outcome"] == "repeat_success"
    assert summary["no_card_events"]["value"] == 1
    assert summary["no_card_events_with_card_key"]["value"] == 1
    assert summary["same_card_had_later_success"]["value"] == 1


def test_no_card_card_history_summary_is_zero_when_no_events():
    summary = {row["metric"]: row for row in no_card_card_history_summary_rows([])}

    assert summary["no_card_events"]["value"] == 0
    assert summary["no_card_events_with_card_key"]["value"] == 0
