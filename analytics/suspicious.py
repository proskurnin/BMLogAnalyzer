from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from analytics.bm_statuses import UNCLASSIFIED_STATUS, classify_bm_status
from analytics.classifiers import classify_code
from analytics.durations import percentile
from analytics.repeats import repeat_attempt_rows
from core.models import PaymentEvent

BURST_WINDOW_SECONDS = 60
BURST_MIN_EVENTS = 3


@dataclass(frozen=True)
class SuspiciousLine:
    source_file: str
    line_number: int
    timestamp: str
    code: int | str
    message: str
    reason: str
    raw_line: str


def suspicious_lines(events: list[PaymentEvent]) -> list[SuspiciousLine]:
    baseline = _normal_baseline(events)
    repeat_reasons = _repeat_reasons(events)
    burst_reasons = _burst_reasons(events)
    rows: list[SuspiciousLine] = []

    for event in events:
        reasons = _event_reasons(event, baseline)
        repeat_reason = repeat_reasons.get((event.source_file, event.line_number))
        if repeat_reason:
            reasons.append(repeat_reason)
        burst_reason = burst_reasons.get((event.source_file, event.line_number))
        if burst_reason:
            reasons.append(burst_reason)
        if not reasons:
            continue
        rows.append(
            SuspiciousLine(
                source_file=event.source_file,
                line_number=event.line_number,
                timestamp=event.timestamp.isoformat(sep=" ") if event.timestamp else "",
                code=event.code if event.code is not None else "missing",
                message=event.message or "",
                reason="; ".join(dict.fromkeys(reasons)),
                raw_line=event.raw_line,
            )
        )

    return sorted(rows, key=lambda item: (item.source_file, item.line_number, item.reason))


def suspicious_line_payloads(events: list[PaymentEvent]) -> list[dict[str, object]]:
    return [line.__dict__ for line in suspicious_lines(events)]


def _event_reasons(event: PaymentEvent, baseline: dict[str, object]) -> list[str]:
    reasons: list[str] = []
    classification = classify_code(event.code)
    status = classify_bm_status(event)

    if classification == "unknown":
        reasons.append("Код результата отсутствует в известной таблице классификации.")
    elif classification == "technical_error":
        reasons.append(f"Строка отличается от нормали: вместо успешного Code:0 получен технический отказ Code:{event.code}.")
    elif classification == "decline":
        reasons.append(f"Строка отличается от нормали: вместо успешного Code:0 получен отказ Code:{event.code}.")

    if classification != "success" and status == UNCLASSIFIED_STATUS:
        reasons.append("BM-статус не удалось отнести к известной нормальной или отказной группе.")

    normal_duration_p95 = baseline.get("duration_p95_ms")
    if event.duration_ms is not None and isinstance(normal_duration_p95, float):
        threshold = max(normal_duration_p95 * 1.5, normal_duration_p95 + 500)
        if event.duration_ms > threshold:
            reasons.append(
                f"Длительность {event.duration_ms:g} ms выше baseline p95 нормальных успешных операций "
                f"({normal_duration_p95:g} ms)."
            )

    if event.duration_ms is None and baseline.get("normal_duration_required"):
        reasons.append("В строке нет duration, хотя у большинства нормальных успешных операций duration присутствует.")

    return reasons


def _normal_baseline(events: list[PaymentEvent]) -> dict[str, object]:
    normal_events = [event for event in events if classify_code(event.code) == "success"]
    normal_durations = [event.duration_ms for event in normal_events if event.duration_ms is not None]
    duration_presence = len(normal_durations) / len(normal_events) if normal_events else 0.0
    return {
        "normal_count": len(normal_events),
        "duration_p95_ms": percentile(normal_durations, 95) if normal_durations else None,
        "normal_duration_required": len(normal_events) >= 3 and duration_presence >= 0.8,
    }


def _repeat_reasons(events: list[PaymentEvent]) -> dict[tuple[str, int], str]:
    reasons: dict[tuple[str, int], str] = {}
    for row in repeat_attempt_rows(events):
        if not row.get("repeat_found_within_3s"):
            continue
        source_file = str(row["source_file"])
        line_number = int(row["failure_line_number"])
        reasons[(source_file, line_number)] = (
            "После неуспешного события найден следующий PaymentStart resp "
            f"через {row['repeat_delay_seconds']} сек. в той же source log."
        )
    return reasons


def _burst_reasons(events: list[PaymentEvent]) -> dict[tuple[str, int], str]:
    grouped: dict[tuple[str, int | None, str], list[PaymentEvent]] = {}
    for event in events:
        if event.timestamp is None or classify_code(event.code) == "success":
            continue
        key = (event.source_file, event.code, event.message or "")
        grouped.setdefault(key, []).append(event)

    reasons: dict[tuple[str, int], str] = {}
    for (_source_file, code, message), group_events in grouped.items():
        ordered = sorted(group_events, key=lambda item: (item.timestamp, item.line_number))
        for index, event in enumerate(ordered):
            if event.timestamp is None:
                continue
            window_end = event.timestamp + timedelta(seconds=BURST_WINDOW_SECONDS)
            window_events = [
                candidate
                for candidate in ordered[index:]
                if candidate.timestamp is not None and candidate.timestamp <= window_end
            ]
            if len(window_events) < BURST_MIN_EVENTS:
                continue
            reason = (
                f"Всплеск одинаковых non-success событий: {len(window_events)} строк "
                f"с Code:{code if code is not None else 'missing'}"
            )
            if message:
                reason += f" и Message:{message}"
            reason += f" в одном source log за {BURST_WINDOW_SECONDS} сек."
            for candidate in window_events:
                reasons[(candidate.source_file, candidate.line_number)] = reason

    return reasons
