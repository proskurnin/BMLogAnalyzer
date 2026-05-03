import gzip
import zipfile

from core.archive_extractor import extract_archives


def test_extracts_gz_archive(tmp_path):
    source = tmp_path / "sample.log.gz"
    extracted = tmp_path / "extracted"
    with gzip.open(source, "wt", encoding="utf-8") as handle:
        handle.write("line\n")

    result = extract_archives(source, extracted)

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

    assert len(result.extracted_files) == 1
    assert (extracted / "logs.zip" / "nested" / "a.log").read_text(encoding="utf-8") == "log line\n"
