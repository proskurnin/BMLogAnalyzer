from __future__ import annotations

import gzip
import zipfile
from collections.abc import Iterable, Iterator
from pathlib import Path

from core.models import LogLine

SUPPORTED_SUFFIXES = {".log", ".gz", ".zip"}


def scan_logs(path: Path | str) -> Iterator[LogLine]:
    root = Path(path)
    for source in _iter_sources(root):
        yield from _read_source(source)


def _iter_sources(root: Path) -> Iterable[Path]:
    if root.is_file() and root.suffix.lower() in SUPPORTED_SUFFIXES:
        yield root
        return

    if root.is_dir():
        for path in sorted(root.rglob("*")):
            if path.is_file() and path.suffix.lower() in SUPPORTED_SUFFIXES:
                yield path


def _read_source(path: Path) -> Iterator[LogLine]:
    suffix = path.suffix.lower()
    if suffix == ".zip":
        yield from _read_zip(path)
    elif suffix == ".gz":
        yield from _iter_text_lines(path, gzip.open(path, "rt", encoding="utf-8", errors="replace"))
    else:
        yield from _iter_text_lines(path, path.open("rt", encoding="utf-8", errors="replace"))


def _read_zip(path: Path) -> Iterator[LogLine]:
    with zipfile.ZipFile(path) as archive:
        for name in sorted(archive.namelist()):
            if name.endswith("/") or Path(name).suffix.lower() not in {".log", ".gz"}:
                continue
            source_name = f"{path}!{name}"
            with archive.open(name) as raw:
                if name.lower().endswith(".gz"):
                    with gzip.open(raw, "rt", encoding="utf-8", errors="replace") as text_stream:
                        yield from _iter_text_lines(source_name, text_stream)
                else:
                    for line_number, raw_line in enumerate(raw, start=1):
                        yield LogLine(source_name, line_number, raw_line.decode("utf-8", errors="replace").rstrip("\n\r"))


def _iter_text_lines(source_file: Path | str, stream) -> Iterator[LogLine]:
    with stream:
        for line_number, line in enumerate(stream, start=1):
            yield LogLine(str(source_file), line_number, line.rstrip("\n\r"))
