from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

from web.history import cleanup_expired_history_artifacts
from web.retention import load_storage_policy, update_storage_policy
from web.uploads import cleanup_expired_upload_storage, store_uploads


def test_storage_policy_defaults_and_persists(tmp_path):
    assert load_storage_policy(tmp_path).archive_retention_days == 10

    policy = update_storage_policy(archive_retention_days=7, storage_dir=tmp_path)
    assert policy.archive_retention_days == 7
    assert load_storage_policy(tmp_path).archive_retention_days == 7
    assert json.loads((tmp_path / "web_settings" / "storage_policy.json").read_text(encoding="utf-8"))["archive_retention_days"] == 7


def test_cleanup_expired_upload_storage_removes_files_but_keeps_metadata(tmp_path, monkeypatch):
    upload_root = tmp_path / "uploads"

    def _root(storage_dir=None):
        upload_root.mkdir(parents=True, exist_ok=True)
        (upload_root / "items").mkdir(parents=True, exist_ok=True)
        (upload_root / "files").mkdir(parents=True, exist_ok=True)
        return upload_root

    monkeypatch.setattr("web.uploads._upload_root", _root)

    stored = store_uploads([("sample.log", b"line\n")], owner_email="user@example.com", owner_name="User")
    upload_id = stored[0].upload_id
    item_path = upload_root / "items" / f"{upload_id}.json"
    payload = json.loads(item_path.read_text(encoding="utf-8"))
    payload["created_at"] = (datetime.now(timezone.utc) - timedelta(days=20)).isoformat(timespec="seconds")
    item_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    workspace = upload_root / "bm-log-analyzer-upload-old"
    (workspace / "extracted").mkdir(parents=True, exist_ok=True)
    old_time = (datetime.now(timezone.utc) - timedelta(days=20)).timestamp()
    os.utime(workspace, (old_time, old_time))

    summary = cleanup_expired_upload_storage(retention_days=10)

    assert summary["expired_uploads"] == 1
    assert summary["expired_upload_workspaces"] == 1
    assert not (upload_root / "files" / upload_id).exists()
    assert not workspace.exists()
    refreshed = json.loads(item_path.read_text(encoding="utf-8"))
    assert refreshed["status"] == "expired"
    assert refreshed["download_url"] == ""


def test_cleanup_expired_history_artifacts_keeps_report_but_removes_workspaces(tmp_path, monkeypatch):
    history_root = tmp_path / "history"

    def _root(storage_dir=None):
        history_root.mkdir(parents=True, exist_ok=True)
        (history_root / "runs").mkdir(parents=True, exist_ok=True)
        return history_root

    monkeypatch.setattr("web.history._history_root", _root)

    run_id = "20260513T092502531103Z-433a831b"
    run_dir = history_root / "runs" / run_id
    (run_dir / "input").mkdir(parents=True, exist_ok=True)
    (run_dir / "extracted").mkdir(parents=True, exist_ok=True)
    report_path = run_dir / "report.html"
    report_path.write_text("<html>ok</html>", encoding="utf-8")
    payload = {
        "run_id": run_id,
        "created_at": (datetime.now(timezone.utc) - timedelta(days=20)).isoformat(timespec="seconds"),
        "mode": "analysis",
        "source": "upload",
        "report_path": str(report_path),
        "report_url": f"/report/{run_id}",
        "manifest_url": f"/report/{run_id}/manifest",
        "owner_email": "user@example.com",
        "owner_name": "User",
        "snapshot": {},
    }
    (history_root / "runs" / f"{run_id}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    summary = cleanup_expired_history_artifacts(retention_days=10)

    assert summary["expired_history_runs"] == 1
    assert summary["expired_history_workspaces"] == 2
    assert not (run_dir / "input").exists()
    assert not (run_dir / "extracted").exists()
    assert report_path.exists()
