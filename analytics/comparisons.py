from __future__ import annotations

from collections import defaultdict

from analytics.classifiers import classify_code
from analytics.counters import analyze_events
from core.models import PaymentEvent

MISSING = "missing"


def comparison_rows(events: list[PaymentEvent], dimension: str) -> list[dict[str, object]]:
    groups: dict[str, list[PaymentEvent]] = defaultdict(list)
    for event in events:
        groups[_dimension_value(event, dimension)].append(event)

    rows: list[dict[str, object]] = []
    for value, group_events in sorted(groups.items()):
        result = analyze_events(group_events)
        rows.append(
            {
                dimension: value,
                "total": result.total,
                "success_count": result.success_count,
                "success_percent": result.success_percent,
                "decline_count": result.decline_count,
                "decline_percent": result.decline_percent,
                "technical_error_count": result.technical_error_count,
                "technical_error_percent": result.technical_error_percent,
                "unknown_count": result.unknown_count,
                "unknown_percent": result.unknown_percent,
                "p90_ms": result.p90_ms,
                "p95_ms": result.p95_ms,
            }
        )
    return rows


def code_matrix_rows(events: list[PaymentEvent], dimension: str) -> tuple[list[int | str], list[dict[str, object]]]:
    codes = sorted({event.code if event.code is not None else MISSING for event in events}, key=str)
    groups: dict[str, dict[int | str, int]] = defaultdict(lambda: {code: 0 for code in codes})

    for event in events:
        value = _dimension_value(event, dimension)
        code = event.code if event.code is not None else MISSING
        groups[value][code] += 1

    rows: list[dict[str, object]] = []
    for value, code_counts in sorted(groups.items()):
        row: dict[str, object] = {dimension: value}
        row.update({f"code_{code}": code_counts[code] for code in codes})
        rows.append(row)
    return codes, rows


def classification_matrix_rows(events: list[PaymentEvent], dimension: str) -> list[dict[str, object]]:
    classifications = ("success", "decline", "technical_error", "unknown")
    groups: dict[str, dict[str, int]] = defaultdict(lambda: {classification: 0 for classification in classifications})

    for event in events:
        value = _dimension_value(event, dimension)
        groups[value][classify_code(event.code)] += 1

    rows: list[dict[str, object]] = []
    for value, counts in sorted(groups.items()):
        row: dict[str, object] = {dimension: value}
        row.update(counts)
        rows.append(row)
    return rows


def _dimension_value(event: PaymentEvent, dimension: str) -> str:
    value = getattr(event, dimension)
    return str(value) if value else MISSING
