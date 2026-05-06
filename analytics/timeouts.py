from __future__ import annotations

from analytics.repeat_investigation import failure_repeat_rows, failure_repeat_summary_rows
from core.models import PaymentEvent

TIMEOUT_CODE = 16


def timeout_repeat_rows(events: list[PaymentEvent]) -> list[dict[str, object]]:
    return failure_repeat_rows(
        events,
        failure_code=TIMEOUT_CODE,
        same_error_label="repeat_same_timeout",
    )


def timeout_repeat_summary_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return failure_repeat_summary_rows(
        rows,
        total_metric="timeout_events",
    )
