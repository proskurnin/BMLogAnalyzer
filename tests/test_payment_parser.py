from parsers.payment_parser import parse_payment_start_response


def test_parses_payment_start_resp_line():
    line = (
        "2026-04-29 20:50:41.343 PaymentStart, resp: "
        "{Code:3 Message:Ошибка чтения карты} duration=412 ms p: mgt_nbs-oti-4.4.12"
    )

    event = parse_payment_start_response(line, "test.log", 7)

    assert event is not None
    assert event.source_file == "test.log"
    assert event.line_number == 7
    assert event.timestamp.isoformat(sep=" ") == "2026-04-29 20:50:41.343000"
    assert event.event_type == "PaymentStart resp"
    assert event.code == 3
    assert event.message == "Ошибка чтения карты"
    assert event.duration_ms == 412
    assert event.package == "mgt_nbs-oti-4.4.12"
    assert event.bm_type == "oti"
    assert event.bm_version == "4.4.12"
    assert event.reader_type == "OTI"


def test_parses_payment_start_resp_without_comma_and_tt_package():
    line = (
        "2026-04-29 20:50:41 PaymentStart resp: "
        "{Code:0 Message:OK} duration=250 ms p: mgt_nbs-tt-4.4.6"
    )

    event = parse_payment_start_response(line)

    assert event is not None
    assert event.code == 0
    assert event.message == "OK"
    assert event.duration_ms == 250
    assert event.bm_version == "4.4.6"
    assert event.reader_type == "TT"


def test_handles_missing_duration():
    event = parse_payment_start_response(
        "2026-04-29 20:50:41.343 PaymentStart, resp: {Code:17 Message:Нет карты в поле} p: mgt_nbs-oti-4.4.2"
    )

    assert event is not None
    assert event.duration_ms is None
    assert event.code == 17


def test_parses_structured_bm_log_format():
    line = (
        'time="2026-04-25 11:30:13.686" level=info msg="PaymentStart, resp: 508.11597ms, '
        "{BmTrxId:[49] Code:3 MessageRus:Ошибка чтения карты MessageEng:Card reading error "
        "VirtualCard:{VirtualUid:[]} BmSign:[109]}}, error: no error "
        'p: mgt_nbs-oti-4.4.7, v: .20260127.120207"'
    )

    event = parse_payment_start_response(line)

    assert event is not None
    assert event.timestamp.isoformat(sep=" ") == "2026-04-25 11:30:13.686000"
    assert event.code == 3
    assert event.message == "Ошибка чтения карты"
    assert event.duration_ms == 508.11597
    assert event.bm_version == "4.4.7"
    assert event.reader_type == "OTI"


def test_parses_seconds_duration_as_ms():
    line = (
        'time="2026-04-25 09:01:32.337" level=info msg="PaymentStart, resp: 2.396398195s, '
        "{Code:0 MessageRus:Проходите MessageEng:Go VirtualCard:{VirtualUid:[]}}, "
        'error: no error p: mgt_nbs-oti-4.4.7"'
    )

    event = parse_payment_start_response(line)

    assert event is not None
    assert event.duration_ms == 2396.398195


def test_malformed_non_payment_line_returns_none():
    assert parse_payment_start_response("not a payment line") is None
