from datetime import datetime

from analytics.check_cases import run_builtin_checks
from tests.test_counters import make_event
from tests.test_repeats import _with_event_position


def test_builtin_checks_flag_code_specific_cases():
    events = [
        _with_event_position(make_event(3, message="Ошибка чтения карты"), "a.log", 10, datetime(2026, 4, 29, 10, 0, 0)),
        _with_event_position(make_event(16, message="Истек таймаут"), "a.log", 20, datetime(2026, 4, 29, 10, 1, 0)),
        _with_event_position(make_event(255, message="Операция отклонена"), "a.log", 30, datetime(2026, 4, 29, 10, 2, 0)),
        _with_event_position(make_event(999, message="new code"), "a.log", 40, datetime(2026, 4, 29, 10, 3, 0)),
    ]

    results = run_builtin_checks(events)
    check_ids = {result.check_id for result in results}

    assert "technical_error_code_3" in check_ids
    assert "timeout_code_16" in check_ids
    assert "many_declines_code_255" in check_ids
    assert "unknown_code_detected" in check_ids


def test_builtin_checks_flag_repeat_after_failure():
    failed = _with_event_position(make_event(3), "a.log", 10, datetime(2026, 4, 29, 10, 0, 0))
    repeat = _with_event_position(make_event(0), "a.log", 12, datetime(2026, 4, 29, 10, 0, 2))

    results = run_builtin_checks([failed, repeat])

    repeat_results = [result for result in results if result.check_id == "repeat_after_failure_3s"]
    assert len(repeat_results) == 1
    assert "repeat_delay_seconds=2" in repeat_results[0].evidence
    assert repeat_results[0].source_file == "a.log"
    assert repeat_results[0].line_number == 10
