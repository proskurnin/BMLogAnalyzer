from datetime import datetime, timedelta

from analytics.read_errors import read_error_repeat_rows, read_error_repeat_summary_rows
from tests.test_counters import make_event


def event(code, seconds, message="msg"):
    base = datetime(2026, 4, 29, 20, 50, 41)
    item = make_event(code, message=message)
    return item.__class__(
        source_file=item.source_file,
        line_number=seconds + 1,
        timestamp=base + timedelta(seconds=seconds),
        event_type=item.event_type,
        code=item.code,
        message=item.message,
        duration_ms=item.duration_ms,
        package=item.package,
        bm_type=item.bm_type,
        bm_version=item.bm_version,
        reader_type=item.reader_type,
        reader_firmware=item.reader_firmware,
        raw_line=f"raw {code} {seconds}",
    )


def test_read_error_repeat_rows_classify_repeat_outcomes():
    events = [
        event(3, 0, "Ошибка чтения карты"),
        event(0, 1, "OK"),
        event(3, 10, "Ошибка чтения карты"),
        event(3, 12, "Ошибка чтения карты"),
        event(3, 30, "Ошибка чтения карты"),
        event(0, 40, "OK"),
    ]

    rows = read_error_repeat_rows(events)
    outcomes = [row["repeat_outcome"] for row in rows]

    assert outcomes == ["repeat_success", "repeat_same_read_error", "no_repeat_within_3s", "no_repeat_within_3s"]
    assert rows[0]["repeat_code"] == 0
    assert rows[1]["repeat_code"] == 3
    assert rows[2]["repeat_code"] == ""


def test_read_error_summary_counts_outcomes_and_repeat_codes():
    rows = read_error_repeat_rows([event(3, 0), event(0, 1), event(3, 10), event(3, 20)])
    summary = read_error_repeat_summary_rows(rows)
    by_metric = {row["metric"]: row for row in summary}

    assert by_metric["read_error_events"]["value"] == 3
    assert by_metric["outcome_repeat_success"]["value"] == 1
    assert by_metric["outcome_no_repeat_within_3s"]["value"] == 2
    assert by_metric["repeat_code_0"]["value"] == 1
