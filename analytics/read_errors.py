from __future__ import annotations

from analytics.repeat_investigation import failure_repeat_rows, failure_repeat_summary_rows
from core.models import PaymentEvent

READ_ERROR_CODE = 3


def read_error_repeat_rows(events: list[PaymentEvent]) -> list[dict[str, object]]:
    return failure_repeat_rows(
        events,
        failure_code=READ_ERROR_CODE,
        same_error_label="repeat_same_read_error",
    )


def read_error_repeat_summary_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return failure_repeat_summary_rows(
        rows,
        total_metric="read_error_events",
    )
