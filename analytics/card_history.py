from __future__ import annotations

from collections import Counter, defaultdict

from analytics.card_identity import card_technical_values
from analytics.classifiers import classify_code
from analytics.repeats import repeat_attempt_rows
from core.models import PaymentEvent

READ_ERROR_CODE = 3
TIMEOUT_CODE = 16
NO_CARD_CODE = 17


def card_fingerprint_event_rows(events: list[PaymentEvent]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for event in sorted(events, key=lambda item: (card_fingerprint_key(item), _timestamp_key(item), item.source_file, item.line_number)):
        fingerprint = card_fingerprint(event)
        if not fingerprint["card_key"]:
            continue
        rows.append(
            {
                "card_key": fingerprint["card_key"],
                "key_source": fingerprint["key_source"],
                "source_file": event.source_file,
                "line_number": event.line_number,
                "timestamp": event.timestamp.isoformat(sep=" ") if event.timestamp else "",
                "classification": classify_code(event.code),
                "code": event.code if event.code is not None else "",
                "message": event.message or "",
                "bm_version": event.bm_version or "",
                "reader_type": event.reader_type or "",
                "bin": fingerprint["bin"],
                "hashpan_present": "yes" if fingerprint["hashpan"] else "no",
                "virtual_uid_present": "yes" if fingerprint["virtual_uid"] else "no",
                "virtual_app_code": fingerprint["virtual_app_code"],
                "raw_line": event.raw_line,
            }
        )
    return rows


def read_error_card_history_rows(events: list[PaymentEvent]) -> list[dict[str, object]]:
    return failure_card_history_rows(events, failure_code=READ_ERROR_CODE)


def timeout_card_history_rows(events: list[PaymentEvent]) -> list[dict[str, object]]:
    return failure_card_history_rows(events, failure_code=TIMEOUT_CODE)


def no_card_card_history_rows(events: list[PaymentEvent]) -> list[dict[str, object]]:
    return failure_card_history_rows(events, failure_code=NO_CARD_CODE)


def failure_card_history_rows(events: list[PaymentEvent], *, failure_code: int) -> list[dict[str, object]]:
    keyed_events = _events_by_card_key(events)
    repeat_rows = {
        (row["source_file"], row["failure_line_number"]): row
        for row in repeat_attempt_rows(events)
        if row["failure_code"] == failure_code
    }
    rows: list[dict[str, object]] = []
    for event in sorted(events, key=lambda item: (_timestamp_key(item), item.source_file, item.line_number)):
        if event.code != failure_code:
            continue
        fingerprint = card_fingerprint(event)
        same_card_events = keyed_events.get(fingerprint["card_key"], []) if fingerprint["card_key"] else []
        previous_events = _events_before(same_card_events, event)
        later_events = _events_after(same_card_events, event)
        repeat = repeat_rows.get((event.source_file, event.line_number), {})
        rows.append(
            {
                "source_file": event.source_file,
                "line_number": event.line_number,
                "timestamp": event.timestamp.isoformat(sep=" ") if event.timestamp else "",
                "card_key": fingerprint["card_key"],
                "key_source": fingerprint["key_source"],
                "same_card_events_total": len(same_card_events),
                "same_card_previous_events": len(previous_events),
                "same_card_previous_success": _count_by_classification(previous_events, "success"),
                "same_card_later_events": len(later_events),
                "same_card_later_success": _count_by_classification(later_events, "success"),
                "repeat_found_within_3s": repeat.get("repeat_found_within_3s", False),
                "repeat_outcome": _repeat_outcome_label(repeat, failure_code=failure_code),
                "repeat_code": repeat.get("repeat_code", ""),
                "repeat_message": repeat.get("repeat_message", ""),
                "bin": fingerprint["bin"],
                "hashpan_present": "yes" if fingerprint["hashpan"] else "no",
                "virtual_uid_present": "yes" if fingerprint["virtual_uid"] else "no",
                "virtual_app_code": fingerprint["virtual_app_code"],
                "raw_line": event.raw_line,
            }
        )
    return rows


def read_error_card_history_summary_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return failure_card_history_summary_rows(
        rows,
        total_metric="read_error_events",
        with_key_metric="read_error_events_with_card_key",
        same_error_metric="repeat_same_read_error_within_3s",
        same_error_outcome="repeat_same_read_error",
    )


def timeout_card_history_summary_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return failure_card_history_summary_rows(
        rows,
        total_metric="timeout_events",
        with_key_metric="timeout_events_with_card_key",
        same_error_metric="repeat_same_timeout_within_3s",
        same_error_outcome="repeat_same_timeout",
    )


def no_card_card_history_summary_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return failure_card_history_summary_rows(
        rows,
        total_metric="no_card_events",
        with_key_metric="no_card_events_with_card_key",
        same_error_metric="repeat_same_no_card_within_3s",
        same_error_outcome="repeat_same_no_card",
    )


def failure_card_history_summary_rows(
    rows: list[dict[str, object]],
    *,
    total_metric: str,
    with_key_metric: str,
    same_error_metric: str,
    same_error_outcome: str,
) -> list[dict[str, object]]:
    total = len(rows)
    with_key = sum(1 for row in rows if row["card_key"])
    previous_success = sum(1 for row in rows if int(row["same_card_previous_success"]) > 0)
    later_success = sum(1 for row in rows if int(row["same_card_later_success"]) > 0)
    repeat_success = sum(1 for row in rows if row["repeat_outcome"] == "repeat_success")
    repeat_same_error = sum(1 for row in rows if row["repeat_outcome"] == same_error_outcome)
    by_key_source = Counter(str(row["key_source"] or "missing") for row in rows)
    summary: list[dict[str, object]] = [
        {"metric": total_metric, "value": total, "percent": 100.0 if total else 0.0, "message": ""},
        {
            "metric": with_key_metric,
            "value": with_key,
            "percent": _percent(with_key, total),
            "message": "technical card key from HashPan, VirtualUid/VirtualAppCode, or Bin/VirtualAppCode",
        },
        {
            "metric": "same_card_had_previous_success",
            "value": previous_success,
            "percent": _percent(previous_success, total),
            "message": "",
        },
        {
            "metric": "same_card_had_later_success",
            "value": later_success,
            "percent": _percent(later_success, total),
            "message": "",
        },
        {
            "metric": "repeat_success_within_3s",
            "value": repeat_success,
            "percent": _percent(repeat_success, total),
            "message": "",
        },
        {
            "metric": same_error_metric,
            "value": repeat_same_error,
            "percent": _percent(repeat_same_error, total),
            "message": "",
        },
    ]
    for key_source, count in sorted(by_key_source.items()):
        summary.append(
            {
                "metric": f"key_source_{key_source}",
                "value": count,
                "percent": _percent(count, total),
                "message": "",
            }
        )
    return summary


def card_fingerprint(event: PaymentEvent) -> dict[str, str]:
    text = " ".join(part for part in (event.message, event.raw_line) if part)
    values = card_technical_values(text)
    hashpan = values["hashpan"]
    virtual_uid = values["virtual_uid"]
    virtual_app_code = values["virtual_app_code"]
    bin_value = values["bin"]
    if hashpan:
        key_source = "hashpan"
        card_key = f"hashpan:{hashpan}"
    elif virtual_uid and virtual_app_code:
        key_source = "virtual_uid_app"
        card_key = f"virtual_uid_app:{virtual_uid}:{virtual_app_code}"
    elif virtual_uid:
        key_source = "virtual_uid"
        card_key = f"virtual_uid:{virtual_uid}"
    elif bin_value and virtual_app_code:
        key_source = "bin_app"
        card_key = f"bin_app:{bin_value}:{virtual_app_code}"
    else:
        key_source = ""
        card_key = ""
    return {
        "card_key": card_key,
        "key_source": key_source,
        "bin": bin_value,
        "hashpan": hashpan,
        "virtual_uid": virtual_uid,
        "virtual_app_code": virtual_app_code,
    }


def card_fingerprint_key(event: PaymentEvent) -> str:
    return card_fingerprint(event)["card_key"]


def _events_by_card_key(events: list[PaymentEvent]) -> dict[str, list[PaymentEvent]]:
    grouped: dict[str, list[PaymentEvent]] = defaultdict(list)
    for event in events:
        key = card_fingerprint_key(event)
        if key:
            grouped[key].append(event)
    for key in grouped:
        grouped[key].sort(key=lambda item: (_timestamp_key(item), item.source_file, item.line_number))
    return grouped


def _events_before(events: list[PaymentEvent], target: PaymentEvent) -> list[PaymentEvent]:
    target_key = _event_order_key(target)
    return [event for event in events if _event_order_key(event) < target_key]


def _events_after(events: list[PaymentEvent], target: PaymentEvent) -> list[PaymentEvent]:
    target_key = _event_order_key(target)
    return [event for event in events if _event_order_key(event) > target_key]


def _event_order_key(event: PaymentEvent) -> tuple[str, str, int]:
    return (_timestamp_key(event), event.source_file, event.line_number)


def _timestamp_key(event: PaymentEvent) -> str:
    return event.timestamp.isoformat() if event.timestamp else ""


def _count_by_classification(events: list[PaymentEvent], classification: str) -> int:
    return sum(1 for event in events if classify_code(event.code) == classification)


def _repeat_outcome_label(repeat: dict[str, object], *, failure_code: int) -> str:
    if not repeat:
        return "not_available"
    if not repeat.get("repeat_found_within_3s"):
        return "no_repeat_within_3s"
    if repeat.get("repeat_classification") == "success":
        return "repeat_success"
    if repeat.get("repeat_code") == failure_code:
        if failure_code == TIMEOUT_CODE:
            return "repeat_same_timeout"
        if failure_code == NO_CARD_CODE:
            return "repeat_same_no_card"
        return "repeat_same_read_error"
    repeat_classification = repeat.get("repeat_classification")
    return f"repeat_{repeat_classification}" if repeat_classification else "repeat_unknown"


def _percent(count: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round((count / total) * 100, 2)
