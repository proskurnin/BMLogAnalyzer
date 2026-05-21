from __future__ import annotations

from dataclasses import asdict
from dataclasses import replace
from datetime import datetime, timezone, timedelta
import json
import secrets
import shutil
from pathlib import Path
from typing import Any

from web.history import load_history_run
from web.models import UploadItemModel
from web.retention import load_storage_policy
from web.settings import upload_store_dir

ALLOWED_UPLOAD_SUFFIXES = (".log", ".gz", ".zip", ".tar.gz", ".tgz", ".rar")


def _upload_root(storage_dir: Path | None = None) -> Path:
    root = Path(storage_dir) if storage_dir else upload_store_dir()
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
    owner_email: str = "",
    owner_name: str = "",
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
            owner_email=owner_email,
            owner_name=owner_name,
        )
        _item_path(upload_id, root).write_text(json.dumps(asdict(item), ensure_ascii=False, indent=2), encoding="utf-8")
        uploaded.append(item)
    return uploaded


def allocate_upload_path(original_name: str, storage_dir: Path | None = None) -> tuple[str, Path]:
    root = _upload_root(storage_dir)
    upload_id = new_upload_id()
    stored_path = _file_path(upload_id, original_name, root)
    stored_path.parent.mkdir(parents=True, exist_ok=True)
    return upload_id, stored_path


def save_upload_item(
    *,
    upload_id: str,
    original_name: str,
    stored_path: Path,
    size_bytes: int,
    owner_email: str = "",
    owner_name: str = "",
    storage_dir: Path | None = None,
) -> UploadItemModel:
    root = _upload_root(storage_dir)
    item = UploadItemModel(
        upload_id=upload_id,
        created_at=_utc_now(),
        original_name=original_name,
        stored_path=str(stored_path),
        size_bytes=size_bytes,
        status="stored",
        status_message="",
        download_url=f"/uploads/download/{upload_id}",
        owner_email=owner_email,
        owner_name=owner_name,
    )
    _item_path(upload_id, root).write_text(json.dumps(asdict(item), ensure_ascii=False, indent=2), encoding="utf-8")
    return item


def split_upload_candidates(files: list[tuple[str, bytes]]) -> tuple[list[tuple[str, bytes]], list[tuple[str, bytes]]]:
    accepted: list[tuple[str, bytes]] = []
    rejected: list[tuple[str, bytes]] = []
    for original_name, content in files:
        target = accepted if is_allowed_upload_name(original_name) else rejected
        target.append((original_name, content))
    return accepted, rejected


def is_allowed_upload_name(original_name: str) -> bool:
    lowered = original_name.lower()
    return any(lowered.endswith(suffix) for suffix in ALLOWED_UPLOAD_SUFFIXES)


def list_uploads(
    storage_dir: Path | None = None,
    *,
    limit: int = 200,
    offset: int = 0,
    owner_email: str | None = None,
) -> list[UploadItemModel]:
    items = _all_uploads(storage_dir=storage_dir, owner_email=owner_email)
    if offset < 0:
        offset = 0
    if limit <= 0:
        return items[offset:]
    return items[offset : offset + limit]


def count_uploads(storage_dir: Path | None = None, *, owner_email: str | None = None) -> int:
    return len(_all_uploads(storage_dir=storage_dir, owner_email=owner_email))


def load_upload(upload_id: str, storage_dir: Path | None = None) -> UploadItemModel:
    path = _item_path(upload_id, storage_dir)
    if not path.exists():
        raise FileNotFoundError(upload_id)
    payload = _load_upload_payload(path)
    if payload is None:
        raise FileNotFoundError(upload_id)
    return _decorate_upload_item(UploadItemModel(**payload))


def _all_uploads(storage_dir: Path | None = None, owner_email: str | None = None) -> list[UploadItemModel]:
    root = _upload_root(storage_dir)
    items: list[UploadItemModel] = []
    for path in sorted((root / "items").glob("*.json")):
        payload = _load_upload_payload(path)
        if payload is None:
            continue
        item = _decorate_upload_item(UploadItemModel(**payload))
        if owner_email is None or item.owner_email == owner_email:
            items.append(item)
    items.sort(key=lambda item: item.created_at, reverse=True)
    return items


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
        payload["report_has_ai"] = False
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


def collect_upload_paths(upload_ids: list[str], storage_dir: Path | None = None) -> list[tuple[str, Path]]:
    files: list[tuple[str, Path]] = []
    for upload_id in upload_ids:
        item = load_upload(upload_id, storage_dir)
        stored_path = Path(item.stored_path)
        if stored_path.exists():
            files.append((item.original_name, stored_path))
    return files


def cleanup_expired_upload_storage(*, retention_days: int) -> dict[str, int]:
    root = _upload_root()
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, retention_days))
    expired_uploads = 0
    expired_workspaces = 0

    for path in sorted((root / "items").glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        created_at = _parse_datetime(payload.get("created_at"))
        if created_at is None or created_at > cutoff:
            continue
        expired_uploads += int(_expire_upload_payload(path, payload))

    for workspace in sorted(root.glob("bm-log-analyzer-upload-*")):
        if not workspace.is_dir():
            continue
        if _path_mtime(workspace) <= cutoff:
            shutil.rmtree(workspace, ignore_errors=True)
            expired_workspaces += 1

    return {"expired_uploads": expired_uploads, "expired_upload_workspaces": expired_workspaces}


def summary_from_uploads(uploaded: list[UploadItemModel], rejected_count: int = 0) -> dict[str, Any]:
    total_size = sum(item.size_bytes for item in uploaded)
    uploaded_count = len(uploaded)
    total_size_mb = round(total_size / (1024 * 1024), 2) if total_size else 0.0
    rejected_message = (
        f" {_not_uploaded_phrase(rejected_count)}, потому что они не соответствуют требованиям."
        if rejected_count
        else ""
    )
    message = (
        f"Загружено {uploaded_count} {_archive_word(uploaded_count)} с логами, общим размером {total_size_mb:.2f} Mb. "
        f"Загрузка прошла без ошибок.{rejected_message} Спасибо."
    )
    return {
        "uploaded_count": uploaded_count,
        "rejected_count": rejected_count,
        "total_size_bytes": total_size,
        "total_size_mb": total_size_mb,
        "message": message,
    }


def _archive_word(count: int) -> str:
    if count % 10 == 1 and count % 100 != 11:
        return "архив"
    if count % 10 in {2, 3, 4} and count % 100 not in {12, 13, 14}:
        return "архива"
    return "архивов"


def _file_word(count: int) -> str:
    if count % 10 == 1 and count % 100 != 11:
        return "файл"
    if count % 10 in {2, 3, 4} and count % 100 not in {12, 13, 14}:
        return "файла"
    return "файлов"


def _not_uploaded_phrase(count: int) -> str:
    verb = "не загружен" if count % 10 == 1 and count % 100 != 11 else "не загружены"
    return f"{count} {_file_word(count)} {verb}"


def _decorate_upload_item(item: UploadItemModel) -> UploadItemModel:
    item = _apply_upload_storage_state(item)
    item = _apply_upload_report_state(item)
    item = _apply_upload_retention_state(item)
    return item


def _load_upload_payload(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, OSError):
        return None
    if not isinstance(payload, dict):
        return None
    payload = dict(payload)
    payload.setdefault("report_run_id", "")
    payload.setdefault("report_url", "")
    payload.setdefault("report_has_ai", False)
    payload.setdefault("report_generated_at", "")
    payload.setdefault("retention_expires_at", "")
    payload.setdefault("retention_note", "")
    payload.setdefault("download_url", "")
    payload.setdefault("status", "ready" if payload.get("report_url") else "stored")
    payload.setdefault("status_message", "")
    payload.setdefault("owner_email", "")
    payload.setdefault("owner_name", "")
    return payload


def _apply_upload_report_state(item: UploadItemModel) -> UploadItemModel:
    if not item.report_run_id:
        return item
    try:
        payload = load_history_run(item.report_run_id)
    except FileNotFoundError:
        return item
    report_path = Path(payload.get("report_path") or "")
    if not report_path.exists():
        return item
    ai_result_path = report_path.with_suffix(".ai.json")
    generated_at = str(payload.get("created_at") or "")
    if ai_result_path.exists():
        return replace(item, report_has_ai=True, report_generated_at=generated_at)
    return replace(item, report_has_ai=False, report_generated_at=generated_at)


def _apply_upload_retention_state(item: UploadItemModel) -> UploadItemModel:
    policy = load_storage_policy()
    created_at = _parse_datetime(item.created_at)
    if created_at is None:
        return replace(item, retention_expires_at="", retention_note="")
    expires_at = created_at + timedelta(days=max(1, policy.archive_retention_days))
    note = _format_retention_note(expires_at - datetime.now(timezone.utc))
    return replace(
        item,
        retention_expires_at=expires_at.isoformat(timespec="seconds"),
        retention_note=note,
    )


def _format_retention_note(delta: timedelta) -> str:
    total_seconds = int(delta.total_seconds())
    if total_seconds <= 0:
        return "срок хранения истёк"
    minutes_total = total_seconds // 60
    days, remainder_minutes = divmod(minutes_total, 24 * 60)
    hours, minutes = divmod(remainder_minutes, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days} {_plural_ru(days, 'день', 'дня', 'дней')}")
    if hours:
        parts.append(f"{hours} {_plural_ru(hours, 'час', 'часа', 'часов')}")
    if minutes:
        parts.append(f"{minutes} {_plural_ru(minutes, 'минута', 'минуты', 'минут')}")
    if not parts:
        return "меньше минуты"
    return "до удаления: " + ", ".join(parts)


def _plural_ru(value: int, one: str, few: str, many: str) -> str:
    value = abs(value)
    if value % 10 == 1 and value % 100 != 11:
        return one
    if value % 10 in {2, 3, 4} and value % 100 not in {12, 13, 14}:
        return few
    return many


def _apply_upload_storage_state(item: UploadItemModel) -> UploadItemModel:
    stored_path = Path(item.stored_path)
    if stored_path.exists():
        return item
    if item.download_url or item.status != "expired":
        return replace(item, status="expired", status_message=item.status_message or "Срок хранения истёк", download_url="")
    return item


def _expire_upload_payload(path: Path, payload: dict[str, Any]) -> bool:
    stored_path = Path(payload.get("stored_path") or "")
    if stored_path.exists():
        shutil.rmtree(stored_path.parent, ignore_errors=True)
    payload["status"] = "expired"
    payload["status_message"] = "Срок хранения истёк"
    payload["download_url"] = ""
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _path_mtime(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
