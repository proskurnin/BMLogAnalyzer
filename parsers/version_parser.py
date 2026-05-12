from __future__ import annotations

import re
from dataclasses import dataclass

PACKAGE_RE = re.compile(r"\bmgt_(?:nbs|askp)-(?P<bm_type>oti|tt)-(?P<version>\d+(?:\.\d+)+)\b", re.IGNORECASE)


@dataclass(frozen=True)
class PackageInfo:
    package: str
    bm_type: str
    bm_version: str
    reader_type: str


def parse_package(line: str) -> PackageInfo | None:
    match = PACKAGE_RE.search(line)
    if not match:
        return None

    bm_type = match.group("bm_type").lower()
    return PackageInfo(
        package=match.group(0),
        bm_type=bm_type,
        bm_version=match.group("version"),
        reader_type=bm_type.upper(),
    )
