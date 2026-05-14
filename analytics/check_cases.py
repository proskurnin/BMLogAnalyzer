from __future__ import annotations

from dataclasses import asdict, replace
import json
import os
from pathlib import Path

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

VALID_SEVERITIES = {"info", "warning", "critical"}


def list_check_cases(storage_path: Path | None = None) -> list[CheckCase]:
    return load_check_cases(storage_path)


def load_check_cases(storage_path: Path | None = None) -> list[CheckCase]:
    path = storage_path or _default_check_cases_path()
    if not path.exists():
        return list(BUILTIN_CHECKS)
    payload = json.loads(path.read_text(encoding="utf-8"))
    stored = payload.get("checks", payload) if isinstance(payload, dict) else payload
    if not isinstance(stored, list):
        return list(BUILTIN_CHECKS)
    stored_by_id = {
        str(item.get("check_id")): item
        for item in stored
        if isinstance(item, dict) and str(item.get("check_id") or "").strip()
    }
    checks: list[CheckCase] = []
    for builtin in BUILTIN_CHECKS:
        item = stored_by_id.get(builtin.check_id)
        if item is None:
            checks.append(builtin)
            continue
        checks.append(
            CheckCase(
                check_id=builtin.check_id,
                title=str(item.get("title") or builtin.title).strip() or builtin.title,
                description=str(item.get("description") or builtin.description).strip() or builtin.description,
                severity=_normalize_severity(item.get("severity"), builtin.severity),
                enabled=bool(item.get("enabled", builtin.enabled)),
                version=str(item.get("version") or builtin.version).strip() or builtin.version,
            )
        )
    return checks


def save_check_cases(checks: list[CheckCase], storage_path: Path | None = None) -> None:
    path = storage_path or _default_check_cases_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "bm-log-analyzer.check-cases.v1",
        "checks": [asdict(check) for check in checks],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def update_check_case(
    check_id: str,
    *,
    title: str,
    description: str,
    severity: str,
    enabled: bool,
    storage_path: Path | None = None,
) -> CheckCase:
    checks = load_check_cases(storage_path)
    updated: CheckCase | None = None
    next_checks: list[CheckCase] = []
    for check in checks:
        if check.check_id != check_id:
            next_checks.append(check)
            continue
        updated = replace(
            check,
            title=title.strip() or check.title,
            description=description.strip() or check.description,
            severity=_normalize_severity(severity, check.severity),
            enabled=enabled,
            version=_increment_version(check.version),
        )
        next_checks.append(updated)
    if updated is None:
        raise ValueError(f"unknown check case: {check_id}")
    save_check_cases(next_checks, storage_path)
    return updated


def reset_check_cases(storage_path: Path | None = None) -> None:
    path = storage_path or _default_check_cases_path()
    if path.exists():
        path.unlink()


def run_builtin_checks(events: list[PaymentEvent], checks: list[CheckCase] | None = None) -> list[CheckResult]:
    results: list[CheckResult] = []
    check_by_id = {check.check_id: check for check in (checks or load_check_cases()) if check.enabled}

    for event in events:
        if event.code == 3 and (check := check_by_id.get("technical_error_code_3")):
            results.append(_event_result(check, event, "Code:3"))
        elif event.code == 16 and (check := check_by_id.get("timeout_code_16")):
            results.append(_event_result(check, event, "Code:16"))
        elif event.code == 255 and (check := check_by_id.get("many_declines_code_255")):
            results.append(_event_result(check, event, "Code:255"))
        elif classify_code(event.code) == "unknown" and (check := check_by_id.get("unknown_code_detected")):
            results.append(_event_result(check, event, f"Code:{event.code}"))

    repeat_check = check_by_id.get("repeat_after_failure_3s")
    if repeat_check:
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


def _default_check_cases_path() -> Path:
    configured = os.getenv("BM_CHECK_CASES_PATH", "").strip()
    if configured:
        return Path(configured)
    return Path(os.getenv("BM_DATA_DIR", "./_workdir")) / "web_settings" / "check_cases.json"


def _normalize_severity(value: object, default: str) -> str:
    severity = str(value or "").strip().lower()
    return severity if severity in VALID_SEVERITIES else default


def _increment_version(value: str) -> str:
    try:
        return str(int(value) + 1)
    except ValueError:
        return "1"
