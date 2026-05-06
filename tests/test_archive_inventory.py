import tarfile
import zipfile

from analytics.archive_inventory import (
    archive_category_totals,
    build_archive_inventory,
    classify_archive_member,
    explicit_reader_log_count,
    explicit_system_log_count,
)


def test_classifies_archive_members_by_real_path_category():
    assert classify_archive_member("bm/logs/bm/bm-rotate.log") == "BM rotate"
    assert classify_archive_member("bm/logs/bm-std/bm.current.log") == "BM stdout"
    assert classify_archive_member("bm/logs/stopper/stopper-rotate.log") == "Stopper rotate"
    assert classify_archive_member("bm/logs/stopper-std/stopper.current.log") == "Stopper stdout"
    assert classify_archive_member("17/vil.logs/20260505.log") == "VIL logs"
    assert classify_archive_member("logs/reader/reader.log") == "Reader logs"
    assert classify_archive_member("reader-1.44.6518.bin.P.signed") == "Reader firmware binary"
    assert classify_archive_member("var/log/syslog") == "System logs"
    assert classify_archive_member("bm/bm.service") == "Service config"


def test_builds_archive_inventory_from_zip_members(tmp_path):
    archive_path = tmp_path / "sample.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("bm/logs/bm/bm-rotate-2026-04-29T08-54-59.736.log.gz", "x")
        archive.writestr("bm/logs/stopper/stopper-rotate.log", "x")
        archive.writestr("reader-1.44.6518.bin.P.signed", "x")
        archive.writestr("var/log/syslog", "x")
        archive.writestr("db/transaction.db", "x")

    rows = build_archive_inventory([str(archive_path)])
    totals = archive_category_totals(rows)

    assert totals["BM rotate"] == 1
    assert totals["Stopper rotate"] == 1
    assert totals["Reader firmware binary"] == 1
    assert totals["System logs"] == 1
    assert totals["Other"] == 1
    assert explicit_reader_log_count(rows) == 0
    assert explicit_system_log_count(rows) == 1
    bm_row = next(row for row in rows if row.category == "BM rotate")
    assert bm_row.date_from == "2026-04-29"
    assert bm_row.date_to == "2026-04-29"


def test_builds_archive_inventory_from_tar_gz_members(tmp_path):
    archive_path = tmp_path / "sample.tar.gz"
    payload = tmp_path / "payload.log"
    payload.write_text("x", encoding="utf-8")
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(payload, arcname="17/vil.logs/20260505.log")

    rows = build_archive_inventory([str(archive_path)])

    assert len(rows) == 1
    assert rows[0].category == "VIL logs"
    assert rows[0].date_from == "2026-05-05"
