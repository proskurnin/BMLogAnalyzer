from __future__ import annotations

from analytics.classifiers import classify_code
from analytics.repeats import repeat_attempt_rows
from core.models import CheckCase, CheckResult, PaymentEvent

BUILTIN_CHECKS = [
    CheckCase(
        check_id="repeat_after_failure_3s",
        title="Повторное прикладывание после неуспеха до 3 секунд",
        description="После non-success PaymentStart resp найден следующий PaymentStart resp в том же source log в интервале 0-3 секунды.",
        severity="info",
    ),
    CheckCase(
        check_id="technical_error_code_3",
        title="Ошибка чтения карты",
        description="PaymentStart resp вернул Code:3.",
        severity="warning",
    ),
    CheckCase(
        check_id="timeout_code_16",
        title="Истек таймаут",
        description="PaymentStart resp вернул Code:16.",
        severity="warning",
    ),
    CheckCase(
        check_id="many_declines_code_255",
        title="Операция отклонена",
        description="PaymentStart resp вернул Code:255.",
        severity="info",
    ),
    CheckCase(
        check_id="unknown_code_detected",
        title="Неизвестный код результата",
        description="PaymentStart resp содержит код, которого нет в таблице классификации.",
        severity="critical",
    ),
]


def run_builtin_checks(events: list[PaymentEvent]) -> list[CheckResult]:
    results: list[CheckResult] = []
    check_by_id = {check.check_id: check for check in BUILTIN_CHECKS if check.enabled}

    for event in events:
        if event.code == 3:
            results.append(_event_result(check_by_id["technical_error_code_3"], event, "Code:3"))
        elif event.code == 16:
            results.append(_event_result(check_by_id["timeout_code_16"], event, "Code:16"))
        elif event.code == 255:
            results.append(_event_result(check_by_id["many_declines_code_255"], event, "Code:255"))
        elif classify_code(event.code) == "unknown":
            results.append(_event_result(check_by_id["unknown_code_detected"], event, f"Code:{event.code}"))

    repeat_check = check_by_id["repeat_after_failure_3s"]
    for row in repeat_attempt_rows(events):
        if not row["repeat_found_within_3s"]:
            continue
        results.append(
            CheckResult(
                check_id=repeat_check.check_id,
                title=repeat_check.title,
                severity=repeat_check.severity,
                status="matched",
                source_file=str(row["source_file"]),
                line_number=int(row["failure_line_number"]),
                timestamp=None,
                code=int(row["failure_code"]) if str(row["failure_code"]).isdigit() else None,
                message=str(row["failure_message"]) if row["failure_message"] else None,
                evidence=(
                    f"repeat_delay_seconds={row['repeat_delay_seconds']} "
                    f"repeat_line_number={row['repeat_line_number']} "
                    f"repeat_code={row['repeat_code']}"
                ),
                raw_line=str(row["failure_raw_line"]),
            )
        )

    return sorted(results, key=lambda item: (item.source_file, item.line_number or 0, item.check_id))


def _event_result(check: CheckCase, event: PaymentEvent, evidence: str) -> CheckResult:
    return CheckResult(
        check_id=check.check_id,
        title=check.title,
        severity=check.severity,
        status="matched",
        source_file=event.source_file,
        line_number=event.line_number,
        timestamp=event.timestamp,
        code=event.code,
        message=event.message,
        evidence=evidence,
        raw_line=event.raw_line,
    )
