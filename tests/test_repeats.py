from datetime import datetime, timedelta

from analytics.repeats import repeat_attempt_rows, repeat_attempt_summary_rows
from tests.test_counters import make_event


def test_detects_repeat_attempt_within_three_seconds():
    failed = make_event(3, message="Ошибка чтения карты")
    repeated = make_event(0, message="Проходите")
    failed = _with_event_position(failed, "a.log", 10, datetime(2026, 4, 29, 10, 0, 0))
    repeated = _with_event_position(repeated, "a.log", 12, datetime(2026, 4, 29, 10, 0, 2))

    rows = repeat_attempt_rows([failed, repeated])

    assert len(rows) == 1
    assert rows[0]["repeat_found_within_3s"] is True
    assert rows[0]["repeat_delay_seconds"] == "2"
    assert rows[0]["repeat_line_number"] == 12
    assert rows[0]["repeat_code"] == 0


def test_does_not_detect_repeat_after_three_seconds():
    failed = _with_event_position(make_event(4, message="Карта в стоп-листе"), "a.log", 10, datetime(2026, 4, 29, 10, 0, 0))
    late = _with_event_position(make_event(0, message="Проходите"), "a.log", 12, datetime(2026, 4, 29, 10, 0, 4))

    rows = repeat_attempt_rows([failed, late])

    assert len(rows) == 1
    assert rows[0]["repeat_found_within_3s"] is False
    assert rows[0]["repeat_delay_seconds"] == "4"
    assert rows[0]["repeat_line_number"] == ""


def test_repeat_summary_counts_failed_events():
    failed = _with_event_position(make_event(3), "a.log", 10, datetime(2026, 4, 29, 10, 0, 0))
    repeated = _with_event_position(make_event(0), "a.log", 12, datetime(2026, 4, 29, 10, 0, 1))

    summary = repeat_attempt_summary_rows(repeat_attempt_rows([failed, repeated]))

    assert {"metric": "failed_events", "value": 1} in summary
    assert {"metric": "repeat_found_within_3s", "value": 1} in summary
    assert {"metric": "repeat_not_found_within_3s", "value": 0} in summary


def _with_event_position(event, source_file, line_number, timestamp):
    return type(event)(
        source_file=source_file,
        line_number=line_number,
        timestamp=timestamp,
        event_type=event.event_type,
        code=event.code,
        message=event.message,
        duration_ms=event.duration_ms,
        package=event.package,
        bm_type=event.bm_type,
        bm_version=event.bm_version,
        reader_type=event.reader_type,
        reader_firmware=event.reader_firmware,
        raw_line=event.raw_line,
    )
