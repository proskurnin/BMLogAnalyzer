from __future__ import annotations

from collections import Counter
from datetime import timedelta

from analytics.classifiers import classify_code
from core.models import PaymentEvent

REPEAT_WINDOW_SECONDS = 3


def repeat_attempt_rows(events: list[PaymentEvent], window_seconds: int = REPEAT_WINDOW_SECONDS) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for source_file, source_events in _group_events_by_source(events).items():
        ordered = sorted(
            [event for event in source_events if event.timestamp is not None],
            key=lambda event: (event.timestamp, event.line_number),
        )
        for index, event in enumerate(ordered):
            classification = classify_code(event.code)
            if classification == "success":
                continue
            next_event = ordered[index + 1] if index + 1 < len(ordered) else None
            delta_seconds = _delta_seconds(event, next_event)
            repeat_found = delta_seconds is not None and 0 <= delta_seconds <= window_seconds
            rows.append(
                {
                    "source_file": source_file,
                    "failure_line_number": event.line_number,
                    "failure_timestamp": event.timestamp.isoformat(sep=" ") if event.timestamp else "",
                    "failure_classification": classification,
                    "failure_code": event.code if event.code is not None else "",
                    "failure_message": event.message or "",
                    "repeat_found_within_3s": repeat_found,
                    "repeat_delay_seconds": _format_optional_number(delta_seconds),
                    "repeat_line_number": next_event.line_number if repeat_found and next_event else "",
                    "repeat_timestamp": next_event.timestamp.isoformat(sep=" ") if repeat_found and next_event else "",
                    "repeat_code": next_event.code if repeat_found and next_event and next_event.code is not None else "",
                    "repeat_message": next_event.message if repeat_found and next_event and next_event.message else "",
                    "repeat_classification": classify_code(next_event.code) if repeat_found and next_event else "",
                    "failure_raw_line": event.raw_line,
                    "repeat_raw_line": next_event.raw_line if repeat_found and next_event else "",
                }
            )
    return rows


def repeat_attempt_summary_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    total = len(rows)
    repeated = sum(1 for row in rows if row["repeat_found_within_3s"])
    not_repeated = total - repeated
    by_failure_code = Counter(
        (row["failure_code"], row["failure_message"], row["repeat_found_within_3s"]) for row in rows
    )
    summary = [
        {"metric": "failed_events", "value": total},
        {"metric": "repeat_found_within_3s", "value": repeated},
        {"metric": "repeat_not_found_within_3s", "value": not_repeated},
    ]
    for (code, message, repeat_found), count in sorted(by_failure_code.items(), key=lambda item: str(item[0])):
        suffix = "repeat" if repeat_found else "no_repeat"
        summary.append({"metric": f"code_{code}_{suffix}", "value": count, "message": message})
    return summary


def _group_events_by_source(events: list[PaymentEvent]) -> dict[str, list[PaymentEvent]]:
    grouped: dict[str, list[PaymentEvent]] = {}
    for event in events:
        grouped.setdefault(event.source_file, []).append(event)
    return grouped


def _delta_seconds(event: PaymentEvent, next_event: PaymentEvent | None) -> float | None:
    if event.timestamp is None or next_event is None or next_event.timestamp is None:
        return None
    delta: timedelta = next_event.timestamp - event.timestamp
    return delta.total_seconds()


def _format_optional_number(value: float | None) -> str:
    if value is None:
        return ""
    if value.is_integer():
        return str(int(value))
    return str(round(value, 6))
