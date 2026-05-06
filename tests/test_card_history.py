from dataclasses import replace
from datetime import datetime

from analytics.card_history import (
    card_fingerprint,
    card_fingerprint_event_rows,
    read_error_card_history_rows,
    read_error_card_history_summary_rows,
)
from tests.test_counters import make_event


def test_card_fingerprint_ignores_empty_hashpan_and_virtual_uid_arrays():
    event = replace(
        make_event(16),
        raw_line="PaymentStart resp: {Code:16 Bin:0 HashPan:[] VirtualCard:{VirtualUid:[] VirtualAppCode:0}}",
    )

    fingerprint = card_fingerprint(event)

    assert fingerprint["card_key"] == "bin_app:0:0"
    assert fingerprint["hashpan"] == ""
    assert fingerprint["virtual_uid"] == ""


def test_read_error_card_history_detects_later_success_for_same_hashpan():
    first_error = replace(
        make_event(3, message="Ошибка чтения карты"),
        source_file="bm/a.log",
        line_number=10,
        timestamp=datetime(2026, 5, 1, 10, 0, 0),
        raw_line="PaymentStart resp: {Code:3 MessageRus:Ошибка чтения карты Bin:11111111 HashPan:abc VirtualCard:{VirtualUid: VirtualAppCode:0}}",
    )
    repeat_success = replace(
        make_event(0, message="OK"),
        source_file="bm/a.log",
        line_number=11,
        timestamp=datetime(2026, 5, 1, 10, 0, 2),
        raw_line="PaymentStart resp: {Code:0 MessageRus:OK Bin:11111111 HashPan:abc VirtualCard:{VirtualUid: VirtualAppCode:0}}",
    )

    rows = read_error_card_history_rows([first_error, repeat_success])
    summary = {row["metric"]: row for row in read_error_card_history_summary_rows(rows)}

    assert len(card_fingerprint_event_rows([first_error, repeat_success])) == 2
    assert len(rows) == 1
    assert rows[0]["key_source"] == "hashpan"
    assert rows[0]["same_card_events_total"] == 2
    assert rows[0]["same_card_later_success"] == 1
    assert rows[0]["repeat_outcome"] == "repeat_success"
    assert summary["read_error_events_with_card_key"]["value"] == 1
    assert summary["same_card_had_later_success"]["value"] == 1
    assert summary["repeat_success_within_3s"]["value"] == 1
