from __future__ import annotations

BUCKETS = ("<300 ms", "300-500 ms", "500-1000 ms", "1000-2000 ms", ">2000 ms", "missing duration")


def duration_bucket(duration_ms: int | None) -> str:
    if duration_ms is None:
        return "missing duration"
    if duration_ms < 300:
        return "<300 ms"
    if duration_ms <= 500:
        return "300-500 ms"
    if duration_ms <= 1000:
        return "500-1000 ms"
    if duration_ms <= 2000:
        return "1000-2000 ms"
    return ">2000 ms"


def percentile(values: list[int], percentile_value: int) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = (percentile_value / 100) * (len(ordered) - 1)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = rank - lower
    value = ordered[lower] + (ordered[upper] - ordered[lower]) * fraction
    return round(value, 2)
