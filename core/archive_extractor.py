from __future__ import annotations

import gzip
import shutil
import subprocess
import tarfile
import zipfile
from pathlib import Path

from core.models import ExtractionResult

ARCHIVE_SUFFIXES = {".zip", ".gz", ".tgz", ".rar"}


def extract_archives(input_path: Path | str, extracted_dir: Path | str) -> ExtractionResult:
    root = Path(input_path)
    output_root = Path(extracted_dir)
    _prepare_extracted_dir(output_root)

    extracted_files: list[str] = []
    skipped_files: list[str] = []
    source_archives: list[str] = []

    for archive_path in _iter_archives(root):
        source_archives.append(str(archive_path))
        try:
            if _is_tar_gz(archive_path):
                extracted_files.extend(_extract_tar(archive_path, output_root))
            elif archive_path.suffix.lower() == ".zip":
                extracted_files.extend(_extract_zip(archive_path, output_root))
            elif archive_path.suffix.lower() == ".rar":
                extracted_files.extend(_extract_rar(archive_path, output_root))
            elif archive_path.suffix.lower() == ".gz":
                extracted_files.append(str(_extract_gz(archive_path, output_root)))
        except (OSError, gzip.BadGzipFile, zipfile.BadZipFile, tarfile.TarError, subprocess.SubprocessError):
            skipped_files.append(str(archive_path))

    return ExtractionResult(
        input_path=str(root),
        extracted_dir=str(output_root),
        source_archives=source_archives,
        extracted_files=extracted_files,
        skipped_files=skipped_files,
    )


def _prepare_extracted_dir(output_root: Path) -> None:
    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / ".gitkeep").write_text("\n", encoding="utf-8")


def _iter_archives(root: Path):
    if root.is_file() and _is_archive_candidate(root):
        yield root
        return

    if root.is_dir():
        for path in sorted(root.rglob("*")):
            if path.is_file() and _is_archive_candidate(path):
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
            if not _is_supported_log_member(member_path):
                continue
            target_path = _safe_join(target_root, member_path)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, target_path.open("wb") as target:
                shutil.copyfileobj(source, target)
            extracted.append(str(target_path))

    return extracted


def _extract_tar(path: Path, output_root: Path) -> list[str]:
    target_root = output_root / _safe_stem(path)
    target_root.mkdir(parents=True, exist_ok=True)
    extracted: list[str] = []

    with tarfile.open(path, "r:*") as archive:
        for member in sorted(archive.getmembers(), key=lambda item: item.name):
            if not member.isfile():
                continue
            member_path = Path(member.name)
            if not _is_supported_log_member(member_path):
                continue
            target_path = _safe_join(target_root, member_path)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            source = archive.extractfile(member)
            if source is None:
                continue
            with source, target_path.open("wb") as target:
                shutil.copyfileobj(source, target)
            extracted.append(str(target_path))

    return extracted


def _extract_rar(path: Path, output_root: Path) -> list[str]:
    bsdtar = shutil.which("bsdtar")
    if bsdtar is None:
        raise OSError("bsdtar is required to extract .rar archives")

    target_root = output_root / _safe_stem(path)
    target_root.mkdir(parents=True, exist_ok=True)

    listed = subprocess.run(
        [bsdtar, "-tf", str(path)],
        check=True,
        capture_output=True,
        text=True,
    )
    members = [
        name
        for name in listed.stdout.splitlines()
        if name and _is_supported_log_member(Path(name))
    ]
    if not members:
        return []

    subprocess.run(
        [bsdtar, "-xf", str(path), "-C", str(target_root), *members],
        check=True,
        capture_output=True,
        text=True,
    )

    return [str(_safe_join(target_root, Path(member))) for member in sorted(members)]


def _extract_gz(path: Path, output_root: Path) -> Path:
    target_path = output_root / f"{_safe_stem(path)}.log"
    with gzip.open(path, "rb") as source, target_path.open("wb") as target:
        shutil.copyfileobj(source, target)
    return target_path


def _is_supported_log_member(path: Path) -> bool:
    name = path.name.lower()
    if not _is_safe_member_path(path):
        return False
    if _is_tar_gz(path):
        return False
    return path.suffix.lower() in {".log", ".gz"}


def _is_safe_member_path(path: Path) -> bool:
    return not path.is_absolute() and all(part not in {"", ".", ".."} for part in path.parts)


def _is_archive_candidate(path: Path) -> bool:
    return path.suffix.lower() in ARCHIVE_SUFFIXES or _is_tar_gz(path)


def _is_tar_gz(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith(".tar.gz") or name.endswith(".tgz")


def _safe_stem(path: Path) -> str:
    return path.name.replace("/", "_").replace("\\", "_")


def _safe_join(root: Path, relative_path: Path) -> Path:
    clean_parts = [part for part in relative_path.parts if part not in {"", ".", "..", relative_path.anchor}]
    return root.joinpath(*clean_parts)
