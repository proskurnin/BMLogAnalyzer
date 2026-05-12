from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import json
import secrets
import shutil
from pathlib import Path
from typing import Any

from web.models import UploadItemModel


def _upload_root(storage_dir: Path | None = None) -> Path:
    root = Path(storage_dir or "./_workdir/upload_store")
    root.mkdir(parents=True, exist_ok=True)
    (root / "items").mkdir(parents=True, exist_ok=True)
    (root / "files").mkdir(parents=True, exist_ok=True)
    return root


def _item_path(upload_id: str, storage_dir: Path | None = None) -> Path:
    return _upload_root(storage_dir) / "items" / f"{upload_id}.json"


def _safe_relative_path(original_name: str) -> Path:
    path = Path(original_name)
    parts = [part.replace(":", "_") for part in path.parts if part not in {"", ".", "..", path.anchor}]
    return Path(*parts) if parts else Path("upload.bin")


def _file_path(upload_id: str, original_name: str, storage_dir: Path | None = None) -> Path:
    return _upload_root(storage_dir) / "files" / upload_id / _safe_relative_path(original_name)


def new_upload_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + secrets.token_hex(4)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def store_uploads(
    files: list[tuple[str, bytes]],
    *,
    storage_dir: Path | None = None,
) -> list[UploadItemModel]:
    root = _upload_root(storage_dir)
    uploaded: list[UploadItemModel] = []
    for original_name, content in files:
        upload_id = new_upload_id()
        stored_path = _file_path(upload_id, original_name, root)
        stored_path.parent.mkdir(parents=True, exist_ok=True)
        stored_path.write_bytes(content)
        item = UploadItemModel(
            upload_id=upload_id,
            created_at=_utc_now(),
            original_name=original_name,
            stored_path=str(stored_path),
            size_bytes=len(content),
            status="stored",
            status_message="",
            download_url=f"/uploads/download/{upload_id}",
        )
        _item_path(upload_id, root).write_text(json.dumps(asdict(item), ensure_ascii=False, indent=2), encoding="utf-8")
        uploaded.append(item)
    return uploaded


def list_uploads(
    storage_dir: Path | None = None,
    *,
    limit: int = 200,
) -> list[UploadItemModel]:
    root = _upload_root(storage_dir)
    items: list[UploadItemModel] = []
    for path in sorted((root / "items").glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload.setdefault("report_run_id", "")
        payload.setdefault("report_url", "")
        payload.setdefault("download_url", "")
        payload.setdefault("status", "ready" if payload.get("report_url") else "stored")
        payload.setdefault("status_message", "")
        items.append(UploadItemModel(**payload))
    items.sort(key=lambda item: item.created_at, reverse=True)
    return items[:limit]


def load_upload(upload_id: str, storage_dir: Path | None = None) -> UploadItemModel:
    path = _item_path(upload_id, storage_dir)
    if not path.exists():
        raise FileNotFoundError(upload_id)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.setdefault("report_run_id", "")
    payload.setdefault("report_url", "")
    payload.setdefault("download_url", f"/uploads/download/{upload_id}")
    payload.setdefault("status", "ready" if payload.get("report_url") else "stored")
    payload.setdefault("status_message", "")
    return UploadItemModel(**payload)


def update_upload_status(
    upload_id: str,
    *,
    status: str,
    status_message: str = "",
    storage_dir: Path | None = None,
) -> None:
    root = _upload_root(storage_dir)
    path = _item_path(upload_id, root)
    if not path.exists():
        return
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["status"] = status
    payload["status_message"] = status_message
    payload["download_url"] = payload.get("download_url") or f"/uploads/download/{upload_id}"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def update_upload_reports(
    upload_ids: list[str],
    *,
    report_run_id: str,
    report_url: str,
    storage_dir: Path | None = None,
) -> None:
    root = _upload_root(storage_dir)
    for upload_id in upload_ids:
        path = _item_path(upload_id, root)
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["status"] = "ready"
        payload["status_message"] = ""
        payload["report_run_id"] = report_run_id
        payload["report_url"] = report_url
        payload["download_url"] = payload.get("download_url") or f"/uploads/download/{upload_id}"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def delete_upload(upload_id: str, storage_dir: Path | None = None) -> bool:
    root = _upload_root(storage_dir)
    path = _item_path(upload_id, root)
    if not path.exists():
        return False
    payload = json.loads(path.read_text(encoding="utf-8"))
    stored_path = Path(payload.get("stored_path") or "")
    if stored_path.exists():
        stored_path.unlink()
        parent = stored_path.parent
        if parent.exists():
            shutil.rmtree(parent, ignore_errors=True)
    path.unlink(missing_ok=True)
    return True


def collect_upload_files(upload_ids: list[str], storage_dir: Path | None = None) -> list[tuple[str, bytes]]:
    files: list[tuple[str, bytes]] = []
    for upload_id in upload_ids:
        item = load_upload(upload_id, storage_dir)
        stored_path = Path(item.stored_path)
        files.append((item.original_name, stored_path.read_bytes()))
    return files


def summary_from_uploads(uploaded: list[UploadItemModel]) -> dict[str, Any]:
    total_size = sum(item.size_bytes for item in uploaded)
    return {
        "uploaded_count": len(uploaded),
        "total_size_bytes": total_size,
        "total_size_mb": round(total_size / (1024 * 1024), 2) if total_size else 0.0,
        "message": f"Загружено {len(uploaded)} архивов, общим размером {round(total_size / (1024 * 1024), 2):.2f} Mb. Загрузка прошла без ошибок. Спасибо." if uploaded else "Загружено 0 архивов, общим размером 0.00 Mb. Загрузка прошла без ошибок. Спасибо.",
    }
