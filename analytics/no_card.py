from __future__ import annotations

from analytics.repeat_investigation import failure_repeat_rows, failure_repeat_summary_rows
from core.models import PaymentEvent

NO_CARD_CODE = 17


def no_card_repeat_rows(events: list[PaymentEvent]) -> list[dict[str, object]]:
    return failure_repeat_rows(
        events,
        failure_code=NO_CARD_CODE,
        same_error_label="repeat_same_no_card",
    )


def no_card_repeat_summary_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return failure_repeat_summary_rows(
        rows,
        total_metric="no_card_events",
    )
