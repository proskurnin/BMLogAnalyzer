from __future__ import annotations

import re

FIRMWARE_RE = re.compile(
    r"(?:firmware|fw|reader[_ -]?firmware|reader[_ -]?fw)[:= ]+(?P<version>\d+(?:\.\d+)+)",
    re.IGNORECASE,
)


def parse_reader_firmware(line: str) -> str | None:
    match = FIRMWARE_RE.search(line)
    if not match:
        return None
    return match.group("version")
