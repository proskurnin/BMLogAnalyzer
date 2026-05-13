from __future__ import annotations

import re
from dataclasses import dataclass

PACKAGE_RE = re.compile(
    r"\b(?P<carrier>[A-Za-z0-9_]+)-(?P<platform>[A-Za-z0-9_]+)-(?P<version>\d+(?:\.\d+)+)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class PackageInfo:
    package: str
    carrier: str
    platform: str
    bm_version: str
    reader_type: str | None
    bm_type: str | None


def parse_package(line: str) -> PackageInfo | None:
    match = PACKAGE_RE.search(line)
    if not match:
        return None

    carrier = match.group("carrier").lower()
    platform = match.group("platform").lower()
    reader_type = platform.upper() if platform in {"oti", "tt"} else None
    return PackageInfo(
        package=match.group(0),
        carrier=carrier,
        platform=platform,
        bm_version=match.group("version"),
        reader_type=reader_type,
        bm_type=platform if reader_type else None,
    )
