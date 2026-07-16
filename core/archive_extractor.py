from __future__ import annotations

import json
import gzip
import hashlib
import shutil
import subprocess
import tarfile
import zipfile
from collections import deque
from pathlib import Path
from time import perf_counter

from core.models import ExtractionArchiveStat, ExtractionResult

ARCHIVE_SUFFIXES = {".zip", ".gz", ".tgz", ".rar"}
CACHE_MANIFEST = "manifest.json"
CACHE_SCHEMA_VERSION = 2


def extract_archives(input_path: Path | str, extracted_dir: Path | str, *, cache_dir: Path | str | None = None) -> ExtractionResult:
    root = Path(input_path)
    output_root = Path(extracted_dir)
    _prepare_extracted_dir(output_root)
    cache_root = Path(cache_dir) if cache_dir else None
    if cache_root:
        cache_root.mkdir(parents=True, exist_ok=True)

    extracted_files: list[str] = []
    extracted_file_origins: dict[str, str] = {}
    skipped_files: list[str] = []
    archive_stats: list[ExtractionArchiveStat] = []
    cache_hits = 0
    cache_misses = 0
    source_archives = [str(path) for path in _iter_archives(root)]
    for archive_path in (Path(path) for path in source_archives):
        extraction = _extract_origin_archive(archive_path, output_root, cache_root=cache_root)
        extracted_files.extend(extraction.extracted_files)
        extracted_file_origins.update({path: str(archive_path) for path in extraction.extracted_files})
        skipped_files.extend(extraction.skipped_files)
        archive_stats.extend(extraction.archive_stats)
        cache_hits += extraction.cache_hits
        cache_misses += extraction.cache_misses

    return ExtractionResult(
        input_path=str(root),
        extracted_dir=str(output_root),
        source_archives=source_archives,
        extracted_files=extracted_files,
        extracted_file_origins=extracted_file_origins,
        skipped_files=skipped_files,
        archive_stats=archive_stats,
        cache_hits=cache_hits,
        cache_misses=cache_misses,
    )


def _extract_origin_archive(archive_path: Path, output_root: Path, *, cache_root: Path | None) -> ExtractionResult:
    cache_key = _archive_cache_key(archive_path) if cache_root else None
    started_at = perf_counter()
    if cache_root and cache_key:
        cached = _restore_cached_extraction(cache_root / cache_key, output_root)
        if cached is not None:
            return ExtractionResult(
                input_path=str(archive_path),
                extracted_dir=str(output_root),
                source_archives=[str(archive_path)],
                extracted_files=cached,
                extracted_file_origins={path: str(archive_path) for path in cached},
                archive_stats=[
                    ExtractionArchiveStat(
                        source_archive=str(archive_path),
                        origin_archive=str(archive_path),
                        archive_type=_archive_type(archive_path),
                        status="ok",
                        duration_ms=(perf_counter() - started_at) * 1000,
                        extracted_files=len(cached),
                        skipped_files=0,
                        size_bytes=_file_size(archive_path),
                        cache_status="hit",
                    )
                ],
                cache_hits=1,
            )

    cache_status = "miss" if cache_root and cache_key else "not_used"
    cache_misses = 1 if cache_status == "miss" else 0
    extraction = _extract_origin_archive_uncached(archive_path, output_root, cache_status=cache_status, cache_misses=cache_misses)
    if cache_root and cache_key:
        if not extraction.skipped_files:
            _store_cached_extraction(cache_root / cache_key, output_root, extraction.extracted_files)
    return extraction


def _extract_origin_archive_uncached(
    archive_path: Path,
    output_root: Path,
    *,
    cache_status: str,
    cache_misses: int,
) -> ExtractionResult:
    extracted_files: list[str] = []
    skipped_files: list[str] = []
    archive_stats: list[ExtractionArchiveStat] = []
    pending = deque([(archive_path, str(archive_path))])
    processed: set[str] = set()

    while pending:
        current_archive, origin_archive = pending.popleft()
        archive_key = str(current_archive)
        if archive_key in processed:
            continue
        processed.add(archive_key)
        started_at = perf_counter()
        status = "ok"
        extracted: list[str] = []
        try:
            extracted = _extract_single_archive(current_archive, output_root)
        except (OSError, gzip.BadGzipFile, zipfile.BadZipFile, tarfile.TarError, subprocess.SubprocessError):
            status = "skipped"
            skipped_files.append(str(current_archive))

        regular_extracted = 0
        for extracted_path in extracted:
            path = Path(extracted_path)
            if _is_archive_candidate(path):
                pending.append((path, origin_archive))
            else:
                regular_extracted += 1
                extracted_files.append(str(path))

        archive_stats.append(
            ExtractionArchiveStat(
                source_archive=str(current_archive),
                origin_archive=origin_archive,
                archive_type=_archive_type(current_archive),
                status=status,
                duration_ms=(perf_counter() - started_at) * 1000,
                extracted_files=regular_extracted,
                skipped_files=1 if status == "skipped" else 0,
                size_bytes=_file_size(current_archive),
                cache_status=cache_status,
            )
        )

    return ExtractionResult(
        input_path=str(archive_path),
        extracted_dir=str(output_root),
        source_archives=[str(archive_path)],
        extracted_files=extracted_files,
        extracted_file_origins={path: str(archive_path) for path in extracted_files},
        skipped_files=skipped_files,
        archive_stats=archive_stats,
        cache_misses=cache_misses,
    )


def _extract_single_archive(archive_path: Path, output_root: Path) -> list[str]:
    if _is_tar_gz(archive_path):
        return _extract_tar(archive_path, output_root)
    if archive_path.suffix.lower() == ".zip":
        return _extract_zip(archive_path, output_root)
    if archive_path.suffix.lower() == ".rar":
        return _extract_rar(archive_path, output_root)
    if archive_path.suffix.lower() == ".gz":
        return [str(_extract_gz(archive_path, output_root))]
    return []


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

    try:
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
    except (OSError, RuntimeError, zipfile.BadZipFile):
        return _extract_zip_with_bsdtar(path, output_root)

    return extracted


def _extract_zip_with_bsdtar(path: Path, output_root: Path) -> list[str]:
    bsdtar = shutil.which("bsdtar")
    if bsdtar is None:
        raise zipfile.BadZipFile(f"cannot extract damaged zip without bsdtar: {path}")

    target_root = output_root / _safe_stem(path)
    target_root.mkdir(parents=True, exist_ok=True)

    listed = subprocess.run(
        [bsdtar, "-tf", str(path)],
        check=False,
        capture_output=True,
        text=True,
    )
    members = [
        name
        for name in listed.stdout.splitlines()
        if name and _is_supported_log_member(Path(name))
    ]
    if not members:
        raise zipfile.BadZipFile(f"no supported members listed in damaged zip: {path}")

    subprocess.run(
        [bsdtar, "-xf", str(path), "-C", str(target_root), *members],
        check=False,
        capture_output=True,
        text=True,
    )

    extracted = []
    for member in sorted(members):
        target_path = _safe_join(target_root, Path(member))
        if target_path.is_file():
            extracted.append(str(target_path))
    if not extracted:
        raise zipfile.BadZipFile(f"no supported members extracted from damaged zip: {path}")
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
    target_path = _gz_target_path(path, output_root)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with gzip.open(path, "rb") as source, target_path.open("wb") as target:
            shutil.copyfileobj(source, target)
    except (OSError, EOFError, gzip.BadGzipFile):
        with path.open("rb") as source, target_path.open("wb") as target:
            shutil.copyfileobj(source, target)
    return target_path


def _gz_target_path(path: Path, output_root: Path) -> Path:
    try:
        relative = path.relative_to(output_root)
    except ValueError:
        return output_root / f"{_safe_stem(path)}.log"
    return output_root / relative.with_name(f"{relative.name}.log")


def _archive_cache_key(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"{_safe_stem(path)}-{digest.hexdigest()[:24]}"


def _restore_cached_extraction(cache_path: Path, output_root: Path) -> list[str] | None:
    manifest_path = cache_path / CACHE_MANIFEST
    files_root = cache_path / "files"
    if not manifest_path.is_file() or not files_root.is_dir():
        return None
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if payload.get("schema_version") != CACHE_SCHEMA_VERSION:
        return None
    relative_files = [str(item) for item in payload.get("files", []) if _is_safe_member_path(Path(str(item)))]
    if not relative_files:
        return []
    restored: list[str] = []
    for relative in relative_files:
        source_path = files_root / relative
        target_path = _safe_join(output_root, Path(relative))
        if not source_path.is_file():
            return None
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)
        restored.append(str(target_path))
    return restored


def _store_cached_extraction(cache_path: Path, output_root: Path, extracted_files: list[str]) -> None:
    temp_path = cache_path.with_name(f"{cache_path.name}.tmp")
    if temp_path.exists():
        shutil.rmtree(temp_path)
    files_root = temp_path / "files"
    files_root.mkdir(parents=True, exist_ok=True)
    relative_files: list[str] = []
    for extracted_file in sorted(extracted_files):
        path = Path(extracted_file)
        try:
            relative = path.relative_to(output_root)
        except ValueError:
            continue
        if not _is_safe_member_path(relative) or not path.is_file():
            continue
        target_path = files_root / relative
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target_path)
        relative_files.append(relative.as_posix())
    (temp_path / CACHE_MANIFEST).write_text(
        json.dumps({"schema_version": CACHE_SCHEMA_VERSION, "files": relative_files}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if cache_path.exists():
        shutil.rmtree(cache_path)
    temp_path.replace(cache_path)


def _archive_type(path: Path) -> str:
    if _is_tar_gz(path):
        return "tar.gz"
    return path.suffix.lower().lstrip(".") or "unknown"


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _is_supported_log_member(path: Path) -> bool:
    if not _is_safe_member_path(path):
        return False
    return path.suffix.lower() == ".log" or _is_stdout_log_member(path) or _is_archive_candidate(path)


def _is_stdout_log_member(path: Path) -> bool:
    parts = {part.lower() for part in path.parts}
    return "bm-std" in parts or "stopper-std" in parts


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
