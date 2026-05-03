from __future__ import annotations

import gzip
import shutil
import zipfile
from pathlib import Path

from core.models import ExtractionResult

ARCHIVE_SUFFIXES = {".zip", ".gz"}


def extract_archives(input_path: Path | str, extracted_dir: Path | str) -> ExtractionResult:
    root = Path(input_path)
    output_root = Path(extracted_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    extracted_files: list[str] = []
    skipped_files: list[str] = []

    for archive_path in _iter_archives(root):
        try:
            if archive_path.suffix.lower() == ".zip":
                extracted_files.extend(_extract_zip(archive_path, output_root))
            elif archive_path.suffix.lower() == ".gz":
                extracted_files.append(str(_extract_gz(archive_path, output_root)))
        except (OSError, gzip.BadGzipFile, zipfile.BadZipFile):
            skipped_files.append(str(archive_path))

    return ExtractionResult(
        input_path=str(root),
        extracted_dir=str(output_root),
        extracted_files=extracted_files,
        skipped_files=skipped_files,
    )


def _iter_archives(root: Path):
    if root.is_file() and root.suffix.lower() in ARCHIVE_SUFFIXES:
        yield root
        return

    if root.is_dir():
        for path in sorted(root.rglob("*")):
            if path.is_file() and path.suffix.lower() in ARCHIVE_SUFFIXES:
                yield path


def _extract_zip(path: Path, output_root: Path) -> list[str]:
    target_root = output_root / _safe_stem(path)
    target_root.mkdir(parents=True, exist_ok=True)
    extracted: list[str] = []

    with zipfile.ZipFile(path) as archive:
        for member in sorted(archive.infolist(), key=lambda item: item.filename):
            if member.is_dir():
                continue
            member_path = Path(member.filename)
            if member_path.suffix.lower() not in {".log", ".gz"}:
                continue
            target_path = _safe_join(target_root, member_path)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, target_path.open("wb") as target:
                shutil.copyfileobj(source, target)
            extracted.append(str(target_path))

    return extracted


def _extract_gz(path: Path, output_root: Path) -> Path:
    target_path = output_root / f"{_safe_stem(path)}.log"
    with gzip.open(path, "rb") as source, target_path.open("wb") as target:
        shutil.copyfileobj(source, target)
    return target_path


def _safe_stem(path: Path) -> str:
    return path.name.replace("/", "_").replace("\\", "_")


def _safe_join(root: Path, relative_path: Path) -> Path:
    clean_parts = [part for part in relative_path.parts if part not in {"", ".", ".."}]
    return root.joinpath(*clean_parts)
