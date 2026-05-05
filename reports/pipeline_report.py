from __future__ import annotations

from time import perf_counter
from typing import Any

from core.models import PipelineStepResult


class ConsolePipelineReporter:
    def __init__(self) -> None:
        self._started_at = perf_counter()

    def callback(self, event: str, payload: PipelineStepResult | str) -> None:
        if event == "start":
            print(f"[PIPELINE] START {payload}", flush=True)
            return
        if event == "finish" and isinstance(payload, PipelineStepResult):
            print(format_pipeline_step(payload), flush=True)

    def print_total(self) -> None:
        duration_ms = (perf_counter() - self._started_at) * 1000
        print(f"[PIPELINE] DONE total_time={_format_duration(duration_ms)}", flush=True)


def format_pipeline_step(step: PipelineStepResult) -> str:
    level = {
        "ok": "OK",
        "completed_with_errors": "WARN",
        "failed": "FAIL",
    }.get(step.status, step.status.upper())
    details = _format_details(step.details)
    suffix = f" {details}" if details else ""
    return (
        f"[PIPELINE] {level} {step.name} "
        f"time={_format_duration(step.duration_ms)} errors={step.errors}{suffix}"
    )


def _format_details(details: dict[str, Any]) -> str:
    parts = []
    for key, value in details.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    return " ".join(parts)


def _format_duration(duration_ms: float) -> str:
    if duration_ms >= 1000:
        return f"{duration_ms / 1000:.2f}s"
    return f"{duration_ms:.1f}ms"
