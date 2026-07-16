import gzip
import json
import subprocess
import tarfile
import zipfile

from core.archive_extractor import _archive_cache_key, extract_archives


def test_extracts_gz_archive(tmp_path):
    source = tmp_path / "sample.log.gz"
    extracted = tmp_path / "extracted"
    with gzip.open(source, "wt", encoding="utf-8") as handle:
        handle.write("line\n")

    result = extract_archives(source, extracted)

    assert result.source_archives == [str(source)]
    assert len(result.extracted_files) == 1
    assert result.skipped_files == []
    assert (extracted / "sample.log.gz.log").read_text(encoding="utf-8") == "line\n"


def test_extracts_log_files_from_zip(tmp_path):
    source = tmp_path / "logs.zip"
    extracted = tmp_path / "extracted"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("nested/a.log", "log line\n")
        archive.writestr("nested/skip.txt", "not a log\n")

    result = extract_archives(source, extracted)

    assert result.source_archives == [str(source)]
    assert len(result.extracted_files) == 1
    assert (extracted / "logs.zip" / "nested" / "a.log").read_text(encoding="utf-8") == "log line\n"


def test_extracts_damaged_zip_with_bsdtar_fallback(tmp_path, monkeypatch):
    source = tmp_path / "damaged.zip"
    extracted = tmp_path / "extracted"
    source.write_bytes(b"damaged zip")
    calls = []

    class BrokenZip:
        def __init__(self, path):
            raise zipfile.BadZipFile("broken")

    def fake_which(name):
        return "/usr/bin/bsdtar" if name == "bsdtar" else None

    def fake_run(args, check, capture_output, text):
        calls.append(args)
        if args[1] == "-tf":
            return subprocess.CompletedProcess(args, 1, stdout="nested/a.log\nnested/skip.txt\n")
        target_root = extracted / "damaged.zip"
        (target_root / "nested").mkdir(parents=True, exist_ok=True)
        (target_root / "nested" / "a.log").write_text("partial log\n", encoding="utf-8")
        return subprocess.CompletedProcess(args, 1, stdout="")

    monkeypatch.setattr("core.archive_extractor.zipfile.ZipFile", BrokenZip)
    monkeypatch.setattr("core.archive_extractor.shutil.which", fake_which)
    monkeypatch.setattr("core.archive_extractor.subprocess.run", fake_run)

    result = extract_archives(source, extracted)

    assert result.extracted_files == [str(extracted / "damaged.zip" / "nested" / "a.log")]
    assert result.skipped_files == []
    assert calls[1] == [
        "/usr/bin/bsdtar",
        "-xf",
        str(source),
        "-C",
        str(extracted / "damaged.zip"),
        "nested/a.log",
    ]


def test_extracts_nested_archives_from_zip(tmp_path):
    source = tmp_path / "logs.zip"
    nested_zip = tmp_path / "nested.zip"
    extracted = tmp_path / "extracted"
    with zipfile.ZipFile(nested_zip, "w") as archive:
        archive.writestr("inner/a.log", "nested log\n")
    with zipfile.ZipFile(source, "w") as archive:
        archive.write(nested_zip, arcname="nested.zip")
        archive.writestr("bm-rotate.log.gz", b"not-real-gzip-but-copied")

    result = extract_archives(source, extracted)

    assert result.extracted_files == [
        str(extracted / "logs.zip" / "bm-rotate.log.gz.log"),
        str(extracted / "nested.zip" / "inner" / "a.log"),
    ]
    assert (extracted / "nested.zip" / "inner" / "a.log").read_text(encoding="utf-8") == "nested log\n"


def test_skips_unsafe_archive_member_paths(tmp_path):
    source = tmp_path / "logs.zip"
    extracted = tmp_path / "extracted"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("../evil.log", "outside\n")
        archive.writestr("/absolute.log", "absolute\n")
        archive.writestr("safe/a.log", "safe\n")

    result = extract_archives(source, extracted)

    assert result.extracted_files == [str(extracted / "logs.zip" / "safe" / "a.log")]
    assert (extracted / "logs.zip" / "safe" / "a.log").read_text(encoding="utf-8") == "safe\n"


def test_cleans_extracted_dir_before_extracting(tmp_path):
    source = tmp_path / "logs.zip"
    extracted = tmp_path / "extracted"
    extracted.mkdir()
    (extracted / "stale.log").write_text("old", encoding="utf-8")
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("fresh.log", "new\n")

    result = extract_archives(source, extracted)

    assert not (extracted / "stale.log").exists()
    assert result.extracted_files == [str(extracted / "logs.zip" / "fresh.log")]


def test_reuses_cached_extraction_for_same_archive(tmp_path):
    source = tmp_path / "logs.zip"
    extracted = tmp_path / "extracted"
    cache_dir = tmp_path / "cache"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("nested/a.log", "first\n")

    first = extract_archives(source, extracted, cache_dir=cache_dir)
    (extracted / "logs.zip" / "nested" / "a.log").unlink()
    second = extract_archives(source, extracted, cache_dir=cache_dir)

    assert first.cache_hits == 0
    assert first.cache_misses == 1
    assert second.cache_hits == 1
    assert second.cache_misses == 0
    assert second.archive_stats[0].cache_status == "hit"
    assert second.extracted_files == [str(extracted / "logs.zip" / "nested" / "a.log")]
    assert (extracted / "logs.zip" / "nested" / "a.log").read_text(encoding="utf-8") == "first\n"


def test_ignores_legacy_archive_cache_without_schema_version(tmp_path):
    source = tmp_path / "logs.zip"
    extracted = tmp_path / "extracted"
    cache_dir = tmp_path / "cache"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("nested/a.log", "fresh\n")
        archive.writestr("nested/b.log", "also fresh\n")

    cache_path = cache_dir / _archive_cache_key(source)
    files_root = cache_path / "files"
    files_root.mkdir(parents=True)
    (files_root / "logs.zip" / "nested").mkdir(parents=True)
    (files_root / "logs.zip" / "nested" / "a.log").write_text("stale\n", encoding="utf-8")
    (cache_path / "manifest.json").write_text(
        json.dumps({"files": ["logs.zip/nested/a.log"]}),
        encoding="utf-8",
    )

    result = extract_archives(source, extracted, cache_dir=cache_dir)

    assert result.cache_hits == 0
    assert result.cache_misses == 1
    assert result.archive_stats[0].cache_status == "miss"
    assert result.extracted_files == [
        str(extracted / "logs.zip" / "nested" / "a.log"),
        str(extracted / "logs.zip" / "nested" / "b.log"),
    ]
    assert (extracted / "logs.zip" / "nested" / "a.log").read_text(encoding="utf-8") == "fresh\n"


def test_extracts_log_files_from_tar_gz(tmp_path):
    source = tmp_path / "bm.tar.gz"
    extracted = tmp_path / "extracted"
    log_source = tmp_path / "a.log"
    skip_source = tmp_path / "skip.txt"
    log_source.write_text("tar log line\n", encoding="utf-8")
    skip_source.write_text("not a log\n", encoding="utf-8")

    with tarfile.open(source, "w:gz") as archive:
        archive.add(log_source, arcname="nested/a.log")
        archive.add(skip_source, arcname="nested/skip.txt")

    result = extract_archives(source, extracted)

    assert result.source_archives == [str(source)]
    assert result.extracted_files == [str(extracted / "bm.tar.gz" / "nested" / "a.log")]
    assert result.skipped_files == []
    assert (extracted / "bm.tar.gz" / "nested" / "a.log").read_text(encoding="utf-8") == "tar log line\n"


def test_skips_invalid_tar_gz_archive(tmp_path):
    source = tmp_path / "bm.tar.gz"
    extracted = tmp_path / "extracted"
    with gzip.open(source, "wt", encoding="utf-8") as handle:
        handle.write("gzip text is not a tar archive\n")

    result = extract_archives(source, extracted)

    assert result.source_archives == [str(source)]
    assert result.extracted_files == []
    assert result.skipped_files == [str(source)]


def test_extracts_log_files_from_rar_with_bsdtar(tmp_path, monkeypatch):
    source = tmp_path / "logs.rar"
    extracted = tmp_path / "extracted"
    source.write_bytes(b"rar placeholder")
    calls = []

    def fake_which(name):
        return "/usr/bin/bsdtar" if name == "bsdtar" else None

    def fake_run(args, check, capture_output, text):
        calls.append(args)
        if args[1] == "-tf":
            return subprocess.CompletedProcess(args, 0, stdout="nested/a.log\nnested/skip.txt\nnested/b.log.gz\n")

        target_root = extracted / "logs.rar"
        (target_root / "nested").mkdir(parents=True, exist_ok=True)
        (target_root / "nested" / "a.log").write_text("rar log line\n", encoding="utf-8")
        (target_root / "nested" / "b.log.gz").write_bytes(b"gz payload")
        return subprocess.CompletedProcess(args, 0, stdout="")

    monkeypatch.setattr("core.archive_extractor.shutil.which", fake_which)
    monkeypatch.setattr("core.archive_extractor.subprocess.run", fake_run)

    result = extract_archives(source, extracted)

    assert result.source_archives == [str(source)]
    assert result.extracted_files == [
        str(extracted / "logs.rar" / "nested" / "a.log"),
        str(extracted / "logs.rar" / "nested" / "b.log.gz.log"),
    ]
    assert result.skipped_files == []
    assert calls[1] == [
        "/usr/bin/bsdtar",
        "-xf",
        str(source),
        "-C",
        str(extracted / "logs.rar"),
        "nested/a.log",
        "nested/b.log.gz",
    ]


def test_skips_rar_when_bsdtar_is_missing(tmp_path, monkeypatch):
    source = tmp_path / "logs.rar"
    extracted = tmp_path / "extracted"
    source.write_bytes(b"rar placeholder")
    monkeypatch.setattr("core.archive_extractor.shutil.which", lambda name: None)

    result = extract_archives(source, extracted)

    assert result.source_archives == [str(source)]
    assert result.extracted_files == []
    assert result.skipped_files == [str(source)]
