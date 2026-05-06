from __future__ import annotations

from collections import Counter

from analytics.card_checks import CARD_CHECK_MARKERS
from analytics.repeat_investigation import repeat_outcome
from analytics.repeats import repeat_attempt_rows
from core.models import PaymentEvent


def oda_cda_repeat_rows(events: list[PaymentEvent]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in repeat_attempt_rows(events):
        text = " ".join(str(row.get(key) or "") for key in ("failure_message", "failure_raw_line"))
        markers = _oda_cda_markers(text)
        if not markers:
            continue
        repeat_found = bool(row["repeat_found_within_3s"])
        repeat_classification = str(row["repeat_classification"]) if row["repeat_classification"] else ""
        failure_code = row["failure_code"]
        rows.append(
            {
                "source_file": row["source_file"],
                "failure_line_number": row["failure_line_number"],
                "failure_timestamp": row["failure_timestamp"],
                "markers": ";".join(markers),
                "failure_code": failure_code,
                "failure_message": row["failure_message"],
                "repeat_found_within_3s": repeat_found,
                "repeat_outcome": repeat_outcome(
                    repeat_found=repeat_found,
                    repeat_classification=repeat_classification,
                    repeat_code=row["repeat_code"],
                    failure_code=failure_code if isinstance(failure_code, int) else -1,
                    same_error_label="repeat_same_oda_cda_error",
                ),
                "repeat_delay_seconds": row["repeat_delay_seconds"],
                "repeat_line_number": row["repeat_line_number"],
                "repeat_timestamp": row["repeat_timestamp"],
                "repeat_code": row["repeat_code"],
                "repeat_message": row["repeat_message"],
                "repeat_classification": repeat_classification,
                "failure_raw_line": row["failure_raw_line"],
                "repeat_raw_line": row["repeat_raw_line"],
            }
        )
    return rows


def oda_cda_repeat_summary_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    total = len(rows)
    by_outcome = Counter(str(row["repeat_outcome"]) for row in rows)
    by_marker = Counter(
        marker
        for row in rows
        for marker in str(row["markers"]).split(";")
        if marker
    )
    summary: list[dict[str, object]] = [
        {"metric": "oda_cda_or_basic_check_events", "value": total, "percent": 100.0 if total else 0.0, "message": ""},
    ]
    if total == 0:
        summary.append(
            {
                "metric": "fact_from_logs",
                "value": 0,
                "percent": 0.0,
                "message": "explicit ODA/CDA/basic-check failure events were not found in repeatable non-success PaymentStart resp events",
            }
        )
        return summary
    for outcome, count in sorted(by_outcome.items()):
        summary.append(
            {
                "metric": f"outcome_{outcome}",
                "value": count,
                "percent": _percent(count, total),
                "message": "",
            }
        )
    for marker, count in sorted(by_marker.items()):
        summary.append(
            {
                "metric": f"marker_{marker}",
                "value": count,
                "percent": _percent(count, total),
                "message": "",
            }
        )
    return summary


def _oda_cda_markers(text: str) -> list[str]:
    return [
        marker
        for marker in ("ODA", "CDA", "basic_check")
        if CARD_CHECK_MARKERS[marker].search(text)
    ]


def _percent(count: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round((count / total) * 100, 2)
