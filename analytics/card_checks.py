from __future__ import annotations

import re
from collections import Counter

from core.models import PaymentEvent

CARD_CHECK_MARKERS = {
    "ODA": re.compile(r"(?<![A-Za-z0-9])ODA(?![A-Za-z0-9])", re.IGNORECASE),
    "CDA": re.compile(r"(?<![A-Za-z0-9])CDA(?![A-Za-z0-9])", re.IGNORECASE),
    "basic_check": re.compile(r"\bbasic[-_ ]?check\b|базов\w*\s+проверк", re.IGNORECASE),
}


def card_check_marker_rows(events: list[PaymentEvent]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for event in events:
        text = " ".join(part for part in (event.message, event.raw_line) if part)
        markers = _matched_markers(text)
        if not markers:
            continue
        rows.append(
            {
                "source_file": event.source_file,
                "line_number": event.line_number,
                "timestamp": event.timestamp.isoformat(sep=" ") if event.timestamp else "",
                "markers": ";".join(markers),
                "code": event.code if event.code is not None else "",
                "message": event.message or "",
                "bm_version": event.bm_version or "",
                "reader_type": event.reader_type or "",
                "raw_line": event.raw_line,
            }
        )
    return rows


def card_check_marker_summary_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    marker_counts: Counter[str] = Counter()
    code_counts: Counter[object] = Counter()
    for row in rows:
        for marker in str(row["markers"]).split(";"):
            if marker:
                marker_counts[marker] += 1
        code_counts[row["code"] if row["code"] != "" else "missing"] += 1

    summary: list[dict[str, object]] = [
        {
            "metric": "explicit_card_check_marker_events",
            "value": len(rows),
            "message": "events with explicit ODA/CDA/basic-check markers",
        }
    ]
    if not rows:
        summary.append(
            {
                "metric": "fact_from_logs",
                "value": 0,
                "message": "explicit ODA/CDA/basic-check markers were not found in parsed PaymentStart resp events",
            }
        )
        return summary

    for marker, count in sorted(marker_counts.items()):
        summary.append({"metric": f"marker_{marker}", "value": count, "message": ""})
    for code, count in sorted(code_counts.items(), key=lambda item: str(item[0])):
        summary.append({"metric": f"code_{code}", "value": count, "message": ""})
    return summary


def _matched_markers(text: str) -> list[str]:
    return [marker for marker, pattern in CARD_CHECK_MARKERS.items() if pattern.search(text)]
