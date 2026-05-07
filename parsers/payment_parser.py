from __future__ import annotations

import re
from decimal import Decimal

from core.models import PaymentEvent
from parsers.reader_parser import parse_reader_firmware
from parsers.timestamp_parser import parse_timestamp
from parsers.version_parser import parse_package

TIMESTAMP_RE = re.compile(r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)")
QUOTED_TIMESTAMP_RE = re.compile(r"\btime=\"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)\"")
PAYMENT_RESP_RE = re.compile(r"\bPaymentStart\s*,?\s*resp\b", re.IGNORECASE)
CODE_RE = re.compile(r"\bCode\s*:\s*(?P<code>-?\d+)\b")
AUTH_TYPE_RE = re.compile(r"\bAuthType\s*:\s*(?P<auth_type>-?\d+)\b")
PAYMENT_TYPE_RE = re.compile(r"\bPaymentType\s*:\s*(?P<payment_type>-?\d+)\b")
MESSAGE_RE = re.compile(
    r"\b(?:Message|MessageRus)\s*:\s*(?P<message>.*?)(?=\s+(?:MessageEng|VirtualCard|BmSign|PaymentType|PassengerId)\s*:|\s*}\s*|\s+duration\s*=|\s+p\s*:|,\s*error:|$)"
)
DURATION_RE = re.compile(r"\bduration\s*=\s*(?P<duration>\d+)\s*ms\b", re.IGNORECASE)
RESP_DURATION_RE = re.compile(
    r"\bPaymentStart\s*,?\s*resp\s*:\s*(?P<value>\d+(?:\.\d+)?)(?P<unit>ms|s)\b",
    re.IGNORECASE,
)


def parse_payment_start_response(line: str, source_file: str = "", line_number: int = 0) -> PaymentEvent | None:
    if not is_payment_start_response_line(line):
        return None

    timestamp_match = TIMESTAMP_RE.search(line) or QUOTED_TIMESTAMP_RE.search(line)
    code_match = CODE_RE.search(line)
    auth_type_match = AUTH_TYPE_RE.search(line)
    payment_type_match = PAYMENT_TYPE_RE.search(line)
    message_match = MESSAGE_RE.search(line)
    package_info = parse_package(line)

    if code_match is None and message_match is None:
        return None

    timestamp = parse_timestamp(timestamp_match.group("timestamp")) if timestamp_match else None
    code = int(code_match.group("code")) if code_match else None
    message = _clean_message(message_match.group("message")) if message_match else None
    duration_ms = parse_duration_ms(line)

    return PaymentEvent(
        source_file=source_file,
        line_number=line_number,
        timestamp=timestamp,
        event_type="PaymentStart resp",
        code=code,
        message=message,
        duration_ms=duration_ms,
        package=package_info.package if package_info else None,
        bm_type=package_info.bm_type if package_info else None,
        bm_version=package_info.bm_version if package_info else None,
        reader_type=package_info.reader_type if package_info else None,
        reader_firmware=parse_reader_firmware(line),
        raw_line=line,
        payment_type=int(payment_type_match.group("payment_type")) if payment_type_match else None,
        auth_type=int(auth_type_match.group("auth_type")) if auth_type_match else None,
    )


def _clean_message(value: str) -> str | None:
    cleaned = value.strip()
    return cleaned or None


def parse_duration_ms(line: str) -> float | None:
    duration_match = DURATION_RE.search(line)
    if duration_match:
        return float(duration_match.group("duration"))

    resp_duration_match = RESP_DURATION_RE.search(line)
    if not resp_duration_match:
        return None

    value = Decimal(resp_duration_match.group("value"))
    unit = resp_duration_match.group("unit").lower()
    if unit == "s":
        return float(value * Decimal("1000"))
    return float(value)


def is_payment_start_response_line(line: str) -> bool:
    return PAYMENT_RESP_RE.search(line) is not None
