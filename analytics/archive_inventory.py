from __future__ import annotations

import re
import tarfile
import zipfile
from collections import Counter, defaultdict
from pathlib import Path

from core.models import ArchiveInventoryRow

CATEGORY_ORDER = [
    "BM rotate",
    "BM stdout",
    "Stopper rotate",
    "Stopper stdout",
    "VIL logs",
    "Reader logs",
    "Reader firmware binary",
    "System logs",
    "Service config",
    "Other log-like",
    "Other",
]

DATE_PATTERNS = (
    re.compile(r"\b(?P<year>20\d{2})-(?P<month>\d{2})-(?P<day>\d{2})(?!\d)"),
    re.compile(r"\b(?P<year>20\d{2})(?P<month>\d{2})(?P<day>\d{2})(?!\d)"),
)


def build_archive_inventory(archives: list[str]) -> list[ArchiveInventoryRow]:
    rows: list[ArchiveInventoryRow] = []
    for archive in sorted(archives):
        rows.extend(_inventory_for_archive(Path(archive)))
    return rows


def archive_category_totals(rows: list[ArchiveInventoryRow]) -> dict[str, int]:
    totals: Counter[str] = Counter()
    for row in rows:
        totals[row.category] += row.count
    return {category: totals[category] for category in CATEGORY_ORDER if totals[category]}


def archive_category_date_range(rows: list[ArchiveInventoryRow], categories: set[str]) -> str:
    dates = sorted(
        date
        for row in rows
        if row.category in categories
        for date in (row.date_from, row.date_to)
        if date
    )
    if not dates:
        return "missing"
    if dates[0] == dates[-1]:
        return dates[0]
    return f"{dates[0]}-{dates[-1]}"


def explicit_reader_log_count(rows: list[ArchiveInventoryRow]) -> int:
    return sum(row.count for row in rows if row.category == "Reader logs")


def explicit_system_log_count(rows: list[ArchiveInventoryRow]) -> int:
    return sum(row.count for row in rows if row.category == "System logs")


def bm_log_count(rows: list[ArchiveInventoryRow]) -> int:
    return sum(row.count for row in rows if row.category in {"BM rotate", "BM stdout"})


def stopper_log_count(rows: list[ArchiveInventoryRow]) -> int:
    return sum(row.count for row in rows if row.category in {"Stopper rotate", "Stopper stdout"})


def _inventory_for_archive(path: Path) -> list[ArchiveInventoryRow]:
    members = _archive_members(path)
    grouped: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for member_name, member_size in members:
        grouped[classify_archive_member(member_name)].append((member_name, member_size))

    rows: list[ArchiveInventoryRow] = []
    for category in CATEGORY_ORDER:
        names = grouped.get(category, [])
        if not names:
            continue
        dates = sorted(date for name, _ in names if (date := _date_from_name(name)))
        rows.append(
            ArchiveInventoryRow(
                archive=str(path),
                category=category,
                count=len(names),
                size_bytes=sum(size for _, size in names),
                date_from=dates[0] if dates else None,
                date_to=dates[-1] if dates else None,
                examples=sorted(name for name, _ in names)[:3],
                files=sorted(name for name, _ in names),
                file_sizes={name: size for name, size in sorted(names)},
            )
        )
    return rows


def classify_archive_member(name: str) -> str:
    path = "/" + name.lower().strip("/")
    basename = Path(path).name

    if "/vil.logs/" in path:
        return "VIL logs"
    if "/logs/bm/" in path or re.search(r"/bm-rotate(?:-|\.log|$)", path):
        return "BM rotate"
    if "/logs/bm-std/" in path:
        return "BM stdout"
    if "/logs/stopper/" in path or re.search(r"/stopper-rotate(?:-|\.log|$)", path):
        return "Stopper rotate"
    if "/logs/stopper-std/" in path:
        return "Stopper stdout"
    if _is_reader_log(path):
        return "Reader logs"
    if re.search(r"/reader-[\d.]+\.bin(?:\.[^/]*)*$", path):
        return "Reader firmware binary"
    if _is_system_log(path):
        return "System logs"
    if basename.endswith(".service"):
        return "Service config"
    if _is_other_log_like(path):
        return "Other log-like"
    return "Other"


def _archive_members(path: Path) -> list[tuple[str, int]]:
    try:
        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path) as archive:
                return sorted(
                    ((info.filename, int(info.file_size)) for info in archive.infolist() if info.filename and not info.is_dir()),
                    key=lambda item: item[0],
                )
        if _is_tar_gz(path):
            with tarfile.open(path, "r:*") as archive:
                return sorted(
                    ((member.name, int(member.size)) for member in archive.getmembers() if member.isfile()),
                    key=lambda item: item[0],
                )
    except (OSError, zipfile.BadZipFile, tarfile.TarError):
        return []
    return [(str(path), path.stat().st_size)] if path.is_file() else []


def _date_from_name(name: str) -> str | None:
    for pattern in DATE_PATTERNS:
        match = pattern.search(name)
        if match:
            return f"{match.group('year')}-{match.group('month')}-{match.group('day')}"
    return None


def _is_reader_log(path: str) -> bool:
    return (
        "/logs/reader/" in path
        or "/reader.logs/" in path
        or "/reader-logs/" in path
        or re.search(r"/reader(?:[-_.].*)?\.log(?:\.gz)?$", path) is not None
    )


def _is_system_log(path: str) -> bool:
    return (
        "/syslog" in path
        or "/journal" in path
        or "/var/log/" in path
        or re.search(r"/(?:messages|kern|kernel|system)\.log(?:\.gz)?$", path) is not None
    )


def _is_other_log_like(path: str) -> bool:
    return path.endswith(".log") or path.endswith(".log.gz")


def _is_tar_gz(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith(".tar.gz") or name.endswith(".tgz")
