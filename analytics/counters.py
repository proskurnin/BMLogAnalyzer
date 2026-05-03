from __future__ import annotations

from collections import Counter
from collections.abc import Iterable

from analytics.classifiers import classify_code
from analytics.durations import BUCKETS, duration_bucket, percentile
from core.models import AnalysisResult, PaymentEvent

MISSING = "missing"


def analyze_events(events: Iterable[PaymentEvent]) -> AnalysisResult:
    event_list = list(events)
    total = len(event_list)

    by_code: Counter[int | str] = Counter(event.code if event.code is not None else MISSING for event in event_list)
    by_message: Counter[str] = Counter(event.message or MISSING for event in event_list)
    by_bm_version: Counter[str] = Counter(event.bm_version or MISSING for event in event_list)
    by_reader_type: Counter[str] = Counter(event.reader_type or MISSING for event in event_list)
    by_reader_firmware: Counter[str] = Counter(event.reader_firmware or MISSING for event in event_list)
    by_classification: Counter[str] = Counter(classify_code(event.code) for event in event_list)
    duration_buckets: Counter[str] = Counter({bucket: 0 for bucket in BUCKETS})
    duration_buckets.update(duration_bucket(event.duration_ms) for event in event_list)

    durations = [event.duration_ms for event in event_list if event.duration_ms is not None]

    success_count = by_classification["success"]
    decline_count = by_classification["decline"]
    technical_error_count = by_classification["technical_error"]
    unknown_count = by_classification["unknown"]

    return AnalysisResult(
        total=total,
        success_count=success_count,
        success_percent=_percent(success_count, total),
        decline_count=decline_count,
        decline_percent=_percent(decline_count, total),
        technical_error_count=technical_error_count,
        technical_error_percent=_percent(technical_error_count, total),
        unknown_count=unknown_count,
        unknown_percent=_percent(unknown_count, total),
        by_code=dict(sorted(by_code.items(), key=lambda item: str(item[0]))),
        by_message=dict(by_message.most_common()),
        by_bm_version=dict(by_bm_version.most_common()),
        by_reader_type=dict(by_reader_type.most_common()),
        by_reader_firmware=dict(by_reader_firmware.most_common()),
        by_classification=dict(by_classification.most_common()),
        duration_buckets=dict(duration_buckets),
        p90_ms=percentile(durations, 90),
        p95_ms=percentile(durations, 95),
    )


def _percent(count: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round((count / total) * 100, 2)
