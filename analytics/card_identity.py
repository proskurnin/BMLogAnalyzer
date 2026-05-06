from __future__ import annotations

import re
from collections import Counter

from core.models import PaymentEvent

EXPLICIT_CARD_TYPE_MARKERS = {
    "MIFARE": re.compile(r"(?<![A-Za-z0-9])MIFARE(?![A-Za-z0-9])", re.IGNORECASE),
    "Troika": re.compile(r"(?<![A-Za-z0-9])Troika(?![A-Za-z0-9])", re.IGNORECASE),
    "Тройка": re.compile(r"(?<![А-Яа-яA-Za-z0-9])Тройка(?![А-Яа-яA-Za-z0-9])", re.IGNORECASE),
    "card_type": re.compile(r"\bcard[-_ ]?type\b|тип\s+карты", re.IGNORECASE),
    "transport_card": re.compile(r"\btransport[-_ ]?card\b|транспортн\w*\s+карт", re.IGNORECASE),
}

TECHNICAL_FIELD_PATTERNS = {
    "bin": re.compile(r"\bBin:\s*(\[[^\]]*\]|[^,\s}]*)", re.IGNORECASE),
    "hashpan": re.compile(r"\bHashPan:\s*(\[[^\]]*\]|[^,\s}]*)", re.IGNORECASE),
    "virtual_uid": re.compile(r"\bVirtualUid:\s*(\[[^\]]*\]|[^,\s}]*)", re.IGNORECASE),
    "virtual_app_code": re.compile(r"\bVirtualAppCode:\s*(\[[^\]]*\]|[^,\s}]*)", re.IGNORECASE),
}

VIRTUAL_CARD_PATTERN = re.compile(r"\bVirtualCard\s*:", re.IGNORECASE)


def card_identity_marker_rows(events: list[PaymentEvent]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for event in events:
        text = " ".join(part for part in (event.message, event.raw_line) if part)
        explicit_markers = _matched_explicit_markers(text)
        technical_values = card_technical_values(text)
        technical_markers = card_technical_markers(text, technical_values)
        if not explicit_markers and not technical_markers:
            continue
        rows.append(
            {
                "source_file": event.source_file,
                "line_number": event.line_number,
                "timestamp": event.timestamp.isoformat(sep=" ") if event.timestamp else "",
                "code": event.code if event.code is not None else "",
                "message": event.message or "",
                "bm_version": event.bm_version or "",
                "reader_type": event.reader_type or "",
                "explicit_card_type_markers": ";".join(explicit_markers),
                "technical_markers": ";".join(technical_markers),
                "bin": technical_values["bin"],
                "hashpan_present": _yes_no(bool(technical_values["hashpan"])),
                "virtual_card_present": _yes_no(VIRTUAL_CARD_PATTERN.search(text) is not None),
                "virtual_uid_present": _yes_no(bool(technical_values["virtual_uid"])),
                "virtual_app_code": technical_values["virtual_app_code"],
                "raw_line": event.raw_line,
            }
        )
    return rows


def card_identity_marker_summary_rows(
    rows: list[dict[str, object]],
    total_events: int,
) -> list[dict[str, object]]:
    explicit_counts: Counter[str] = Counter()
    technical_counts: Counter[str] = Counter()
    for row in rows:
        for marker in str(row["explicit_card_type_markers"]).split(";"):
            if marker:
                explicit_counts[marker] += 1
        for marker in str(row["technical_markers"]).split(";"):
            if marker:
                technical_counts[marker] += 1

    explicit_event_count = sum(1 for row in rows if row["explicit_card_type_markers"])
    summary: list[dict[str, object]] = [
        {
            "metric": "events_analyzed",
            "value": total_events,
            "message": "parsed PaymentStart resp events analyzed",
        },
        {
            "metric": "explicit_card_type_marker_events",
            "value": explicit_event_count,
            "message": "events with explicit MIFARE/Troika/card-type markers",
        },
    ]
    if explicit_event_count == 0:
        summary.append(
            {
                "metric": "fact_from_logs",
                "value": 0,
                "message": "explicit MIFARE/Troika/card-type markers were not found in parsed PaymentStart resp events",
            }
        )
    for marker, count in sorted(explicit_counts.items()):
        summary.append({"metric": f"explicit_marker_{marker}", "value": count, "message": ""})
    for marker, count in sorted(technical_counts.items()):
        summary.append(
            {
                "metric": f"technical_marker_{marker}",
                "value": count,
                "message": "technical field presence only; this does not prove card type",
            }
        )
    return summary


def _matched_explicit_markers(text: str) -> list[str]:
    return [marker for marker, pattern in EXPLICIT_CARD_TYPE_MARKERS.items() if pattern.search(text)]


def card_technical_values(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for field, pattern in TECHNICAL_FIELD_PATTERNS.items():
        match = pattern.search(text)
        values[field] = _normalize_technical_value(match.group(1)) if match else ""
    return values


def card_technical_markers(text: str, values: dict[str, str]) -> list[str]:
    markers: list[str] = []
    if values["bin"]:
        markers.append("bin_present")
    if values["hashpan"]:
        markers.append("hashpan_present")
    if VIRTUAL_CARD_PATTERN.search(text):
        markers.append("virtual_card_present")
    if values["virtual_uid"]:
        markers.append("virtual_uid_present")
    if values["virtual_app_code"]:
        markers.append("virtual_app_code_present")
    return markers


def _normalize_technical_value(value: str) -> str:
    normalized = " ".join(value.strip().split())
    if normalized in {"", "[]"}:
        return ""
    return normalized


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"
