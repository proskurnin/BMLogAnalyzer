from datetime import datetime

from analytics.check_cases import create_check_case, load_check_cases, reset_check_cases, run_builtin_checks, update_check_case
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


def test_check_case_catalog_can_disable_rule(tmp_path):
    path = tmp_path / "check_cases.json"
    update_check_case(
        "technical_error_code_3",
        title="Code 3 custom",
        description="disabled for test",
        severity="critical",
        enabled=False,
        storage_path=path,
    )
    checks = load_check_cases(path)
    disabled = next(check for check in checks if check.check_id == "technical_error_code_3")
    assert disabled.title == "Code 3 custom"
    assert disabled.severity == "critical"
    assert disabled.enabled is False
    assert disabled.version == "2"

    event = _with_event_position(make_event(3, message="Ошибка чтения карты"), "a.log", 10, datetime(2026, 4, 29, 10, 0, 0))
    results = run_builtin_checks([event], checks=checks)
    assert "technical_error_code_3" not in {result.check_id for result in results}

    reset_check_cases(path)
    assert not path.exists()


def test_builtin_check_condition_can_be_redefined(tmp_path):
    path = tmp_path / "check_cases.json"
    update_check_case(
        "technical_error_code_3",
        title="Code 6 redefined",
        description="custom condition for builtin row",
        severity="warning",
        enabled=True,
        condition_type="code",
        condition_value="6",
        storage_path=path,
    )
    checks = load_check_cases(path)
    redefined = next(check for check in checks if check.check_id == "technical_error_code_3")
    assert redefined.condition_type == "code"
    assert redefined.condition_value == "6"

    old_event = _with_event_position(make_event(3, message="Ошибка чтения карты"), "a.log", 10, datetime(2026, 4, 29, 10, 0, 0))
    new_event = _with_event_position(make_event(6, message="Приложите одну карту"), "a.log", 20, datetime(2026, 4, 29, 10, 0, 1))
    results = [result for result in run_builtin_checks([old_event, new_event], checks=checks) if result.check_id == "technical_error_code_3"]

    assert len(results) == 1
    assert results[0].code == 6


def test_custom_check_cases_match_code_message_duration_and_repeat(tmp_path):
    path = tmp_path / "check_cases.json"
    create_check_case(
        title="Custom code 6",
        description="Code 6 custom",
        severity="warning",
        condition_type="code",
        condition_value="6",
        storage_path=path,
    )
    create_check_case(
        title="Custom message",
        description="Message custom",
        severity="info",
        condition_type="message_contains",
        condition_value="карту",
        storage_path=path,
    )
    create_check_case(
        title="Custom duration",
        description="Duration custom",
        severity="critical",
        condition_type="duration_gt",
        condition_value="500",
        storage_path=path,
    )
    create_check_case(
        title="Custom repeat",
        description="Repeat custom",
        severity="warning",
        condition_type="repeat_within_seconds",
        condition_value="3",
        storage_path=path,
    )
    failed = _with_event_position(make_event(6, duration_ms=700, message="Приложите одну карту"), "a.log", 10, datetime(2026, 4, 29, 10, 0, 0))
    repeat = _with_event_position(make_event(0, duration_ms=120), "a.log", 12, datetime(2026, 4, 29, 10, 0, 2))

    results = run_builtin_checks([failed, repeat], checks=load_check_cases(path))
    titles = {result.title for result in results}

    assert "Custom code 6" in titles
    assert "Custom message" in titles
    assert "Custom duration" in titles
    assert "Custom repeat" in titles
