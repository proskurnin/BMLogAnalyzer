from __future__ import annotations

from collections import Counter

from analytics.repeats import repeat_attempt_rows
from core.models import PaymentEvent


def failure_repeat_rows(events: list[PaymentEvent], *, failure_code: int, same_error_label: str) -> list[dict[str, object]]:
    rows = []
    for row in repeat_attempt_rows(events):
        if row["failure_code"] != failure_code:
            continue
        repeat_found = bool(row["repeat_found_within_3s"])
        repeat_classification = str(row["repeat_classification"]) if row["repeat_classification"] else ""
        repeat_code = row["repeat_code"]
        rows.append(
            {
                "source_file": row["source_file"],
                "failure_line_number": row["failure_line_number"],
                "failure_timestamp": row["failure_timestamp"],
                "failure_code": row["failure_code"],
                "failure_message": row["failure_message"],
                "repeat_found_within_3s": repeat_found,
                "repeat_outcome": repeat_outcome(
                    repeat_found=repeat_found,
                    repeat_classification=repeat_classification,
                    repeat_code=repeat_code,
                    failure_code=failure_code,
                    same_error_label=same_error_label,
                ),
                "repeat_delay_seconds": row["repeat_delay_seconds"],
                "repeat_line_number": row["repeat_line_number"],
                "repeat_timestamp": row["repeat_timestamp"],
                "repeat_code": repeat_code,
                "repeat_message": row["repeat_message"],
                "repeat_classification": repeat_classification,
                "failure_raw_line": row["failure_raw_line"],
                "repeat_raw_line": row["repeat_raw_line"],
            }
        )
    return rows


def failure_repeat_summary_rows(
    rows: list[dict[str, object]],
    *,
    total_metric: str,
) -> list[dict[str, object]]:
    total = len(rows)
    by_outcome = Counter(str(row["repeat_outcome"]) for row in rows)
    by_repeat_code = Counter(
        (row["repeat_code"] if row["repeat_code"] != "" else "missing", row["repeat_message"] or "")
        for row in rows
        if row["repeat_found_within_3s"]
    )
    summary = [
        {"metric": total_metric, "value": total, "percent": 100.0 if total else 0.0, "message": ""},
    ]
    for outcome, count in sorted(by_outcome.items()):
        summary.append(
            {
                "metric": f"outcome_{outcome}",
                "value": count,
                "percent": _percent(count, total),
                "message": "",
            }
        )
    for (code, message), count in sorted(by_repeat_code.items(), key=lambda item: str(item[0])):
        summary.append(
            {
                "metric": f"repeat_code_{code}",
                "value": count,
                "percent": _percent(count, total),
                "message": message,
            }
        )
    return summary


def repeat_outcome(
    *,
    repeat_found: bool,
    repeat_classification: str,
    repeat_code: object,
    failure_code: int,
    same_error_label: str,
) -> str:
    if not repeat_found:
        return "no_repeat_within_3s"
    if repeat_classification == "success":
        return "repeat_success"
    if repeat_code == failure_code:
        return same_error_label
    if repeat_classification:
        return f"repeat_{repeat_classification}"
    return "repeat_unknown"


def _percent(count: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round((count / total) * 100, 2)
