from core.models import PaymentEvent
from analytics.classifiers import classify_code
from analytics.counters import analyze_events


def make_event(
    code,
    duration_ms=None,
    bm_version="4.4.12",
    reader_type="OTI",
    message="msg",
    payment_type=None,
    auth_type=None,
):
    return PaymentEvent(
        source_file="test.log",
        line_number=1,
        timestamp=None,
        event_type="PaymentStart resp",
        code=code,
        message=message,
        duration_ms=duration_ms,
        package=f"mgt_nbs-{reader_type.lower()}-{bm_version}" if bm_version else None,
        bm_type=reader_type.lower() if reader_type else None,
        bm_version=bm_version,
        reader_type=reader_type,
        reader_firmware=None,
        raw_line="raw",
        payment_type=payment_type,
        auth_type=auth_type,
    )


def test_classifies_known_codes():
    assert classify_code(0) == "success"
    assert classify_code(1) == "decline"
    assert classify_code(3) == "technical_error"
    assert classify_code(4) == "decline"
    assert classify_code(6) == "decline"
    assert classify_code(16) == "technical_error"
    assert classify_code(17) == "technical_error"
    assert classify_code(255) == "decline"
    assert classify_code(999) == "unknown"
    assert classify_code(None) == "unknown"


def test_calculates_counts_percentages_and_p90_p95():
    events = [
        make_event(0, 100, "4.4.12"),
        make_event(3, 412, "4.4.12"),
        make_event(16, 1200, "4.4.6", "TT"),
        make_event(255, None, "4.4.6", "TT"),
    ]

    result = analyze_events(events)

    assert result.total == 4
    assert result.success_count == 1
    assert result.success_percent == 25.0
    assert result.decline_count == 1
    assert result.decline_percent == 25.0
    assert result.technical_error_count == 2
    assert result.technical_error_percent == 50.0
    assert result.unknown_count == 0
    assert result.unknown_percent == 0.0
    assert result.by_code[0] == 1
    assert result.by_code[3] == 1
    assert result.by_code[16] == 1
    assert result.by_code[255] == 1
    assert result.by_bm_version["4.4.12"] == 2
    assert result.by_bm_version["4.4.6"] == 2
    assert result.duration_buckets["<300 ms"] == 1
    assert result.duration_buckets["300-500 ms"] == 1
    assert result.duration_buckets["1000-2000 ms"] == 1
    assert result.duration_buckets["missing duration"] == 1
    assert result.p90_ms == 1042.4
    assert result.p95_ms == 1121.2


def test_empty_analysis_is_zeroed():
    result = analyze_events([])

    assert result.total == 0
    assert result.success_percent == 0.0
    assert result.p90_ms is None
    assert result.p95_ms is None
