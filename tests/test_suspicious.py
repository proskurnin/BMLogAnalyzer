from datetime import datetime, timedelta
from dataclasses import replace

from analytics.suspicious import suspicious_line_payloads, suspicious_lines
from tests.test_counters import make_event


def _event(code, seconds: int, duration_ms: float | None, message: str = "OK"):
    event = make_event(code, duration_ms=duration_ms, message=message)
    return replace(
        event,
        source_file="bm/a.log",
        line_number=seconds + 1,
        timestamp=datetime(2026, 4, 29, 10, 0, 0) + timedelta(seconds=seconds),
        raw_line=f"PaymentStart resp Code:{code} duration={duration_ms}",
    )


def test_suspicious_lines_use_success_baseline_for_duration_deviation():
    events = [_event(0, second, 100 + second) for second in range(20)]
    events.append(_event(0, 20, 900))

    rows = suspicious_lines(events)

    assert len(rows) == 1
    assert rows[0].line_number == 21
    assert "выше baseline p95" in rows[0].reason


def test_suspicious_lines_flag_non_success_unknown_and_repeats():
    failed = _event(3, 0, 410, "Ошибка чтения карты")
    repeat = _event(0, 2, 120)
    unknown = _event(999, 10, 100, "new code")

    rows = suspicious_line_payloads([failed, repeat, unknown])
    reasons = {int(row["line_number"]): str(row["reason"]) for row in rows}

    assert "технический отказ Code:3" in reasons[1]
    assert "через 2 сек." in reasons[1]
    assert "отсутствует в известной таблице" in reasons[11]


def test_suspicious_lines_flag_same_error_burst_in_source_log():
    events = [
        _event(3, 0, 410, "Ошибка чтения карты"),
        _event(3, 20, 430, "Ошибка чтения карты"),
        _event(3, 40, 450, "Ошибка чтения карты"),
        _event(3, 120, 450, "Ошибка чтения карты"),
    ]

    rows = suspicious_line_payloads(events)
    reasons = {int(row["line_number"]): str(row["reason"]) for row in rows}

    assert "Всплеск одинаковых non-success событий: 3 строк" in reasons[1]
    assert "Message:Ошибка чтения карты" in reasons[21]
    assert "Всплеск одинаковых non-success событий" in reasons[41]
    assert "Всплеск одинаковых non-success событий" not in reasons[121]
