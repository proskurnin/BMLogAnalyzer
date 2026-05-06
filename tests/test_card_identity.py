from dataclasses import replace

from analytics.card_identity import card_identity_marker_rows, card_identity_marker_summary_rows
from tests.test_counters import make_event


def test_card_identity_rows_separate_explicit_and_technical_markers():
    event = replace(
        make_event(3, message="Ошибка чтения карты MIFARE"),
        raw_line=(
            "PaymentStart resp: {Code:3 Message:Ошибка чтения карты MIFARE "
            "Bin:220220 HashPan:abc VirtualCard:{VirtualUid:0102 VirtualAppCode:77}}"
        ),
    )

    rows = card_identity_marker_rows([event])

    assert len(rows) == 1
    assert rows[0]["explicit_card_type_markers"] == "MIFARE"
    assert rows[0]["technical_markers"] == (
        "bin_present;hashpan_present;virtual_card_present;virtual_uid_present;virtual_app_code_present"
    )
    assert rows[0]["bin"] == "220220"
    assert rows[0]["hashpan_present"] == "yes"
    assert rows[0]["virtual_uid_present"] == "yes"
    assert rows[0]["virtual_app_code"] == "77"


def test_card_identity_summary_does_not_infer_card_type_from_technical_fields():
    event = replace(
        make_event(0),
        raw_line="PaymentStart resp: {Code:0 Bin:220220 HashPan:abc VirtualCard:{VirtualUid:0102}}",
    )

    rows = card_identity_marker_rows([event])
    summary = {row["metric"]: row for row in card_identity_marker_summary_rows(rows, total_events=1)}

    assert summary["events_analyzed"]["value"] == 1
    assert summary["explicit_card_type_marker_events"]["value"] == 0
    assert "not found" in summary["fact_from_logs"]["message"]
    assert summary["technical_marker_bin_present"]["value"] == 1
    assert "does not prove card type" in summary["technical_marker_bin_present"]["message"]


def test_card_identity_finds_troika_marker():
    rows = card_identity_marker_rows([make_event(17, message="Нет карты в поле, Тройка")])

    assert rows[0]["explicit_card_type_markers"] == "Тройка"


def test_card_identity_treats_empty_technical_arrays_as_missing_values():
    event = replace(
        make_event(16),
        raw_line="PaymentStart resp: {Code:16 Bin:0 HashPan:[] VirtualCard:{VirtualUid:[] VirtualAppCode:0}}",
    )

    rows = card_identity_marker_rows([event])

    assert rows[0]["hashpan_present"] == "no"
    assert rows[0]["virtual_uid_present"] == "no"
    assert rows[0]["technical_markers"] == "bin_present;virtual_card_present;virtual_app_code_present"
