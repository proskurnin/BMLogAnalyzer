from __future__ import annotations

import gzip
import zipfile
from collections.abc import Iterable, Iterator
from pathlib import Path

from core.models import LogLine

ARCHIVE_SUFFIXES = {".gz", ".zip"}
SUPPORTED_SUFFIXES = {".log", *ARCHIVE_SUFFIXES}


def scan_logs(path: Path | str, *, include_archives: bool = True) -> Iterator[LogLine]:
    root = Path(path)
    for source in iter_log_sources(root, include_archives=include_archives):
        yield from _read_source(source)


def iter_log_sources(root: Path | str, *, include_archives: bool = True) -> Iterable[Path]:
    root = Path(root)
    suffixes = SUPPORTED_SUFFIXES if include_archives else {".log"}
    if root.is_file() and root.suffix.lower() in suffixes and not _is_tar_gz(root):
        yield root
        return

    if root.is_dir():
        for path in sorted(root.rglob("*")):
            if path.is_file() and path.suffix.lower() in suffixes and not _is_tar_gz(path):
                yield path


def _read_source(path: Path) -> Iterator[LogLine]:
    suffix = path.suffix.lower()
    if suffix == ".zip":
        yield from _read_zip(path)
    elif suffix == ".gz":
        yield from _iter_text_lines(path, _open_gzip_or_text(path))
    else:
        yield from _iter_text_lines(path, path.open("rt", encoding="utf-8", errors="replace"))


def _is_tar_gz(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith(".tar.gz") or name.endswith(".tgz")


def _read_zip(path: Path) -> Iterator[LogLine]:
    try:
        with zipfile.ZipFile(path) as archive:
            for name in sorted(archive.namelist()):
                if name.endswith("/") or Path(name).suffix.lower() not in {".log", ".gz"}:
                    continue
                source_name = f"{path}!{name}"
                with archive.open(name) as raw:
                    raw_bytes = raw.read()
                    if name.lower().endswith(".gz"):
                        yield from _iter_text_lines(source_name, _open_gzip_bytes_or_text(raw_bytes))
                    else:
                        yield from _iter_text_bytes(source_name, raw_bytes)
    except zipfile.BadZipFile:
        yield from _iter_text_lines(path, path.open("rt", encoding="utf-8", errors="replace"))


def _iter_text_lines(source_file: Path | str, stream) -> Iterator[LogLine]:
    with stream:
        for line_number, line in enumerate(stream, start=1):
            yield LogLine(str(source_file), line_number, line.rstrip("\n\r"))


def _iter_text_bytes(source_file: Path | str, raw_bytes: bytes) -> Iterator[LogLine]:
    text = raw_bytes.decode("utf-8", errors="replace")
    for line_number, line in enumerate(text.splitlines(), start=1):
        yield LogLine(str(source_file), line_number, line.rstrip("\n\r"))


def _open_gzip_or_text(path: Path):
    raw_bytes = path.read_bytes()
    return _open_gzip_bytes_or_text(raw_bytes)


def _open_gzip_bytes_or_text(raw_bytes: bytes):
    try:
        text = gzip.decompress(raw_bytes).decode("utf-8", errors="replace")
    except (OSError, EOFError, gzip.BadGzipFile):
        text = raw_bytes.decode("utf-8", errors="replace")
    from io import StringIO

    return StringIO(text)
