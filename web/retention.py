from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
from time import monotonic

from web.settings import load_settings

DEFAULT_ARCHIVE_RETENTION_DAYS = 10
DEFAULT_CLEANUP_INTERVAL_SECONDS = 60 * 60
_last_cleanup_at = 0.0


@dataclass(frozen=True)
class StoragePolicyModel:
    archive_retention_days: int = DEFAULT_ARCHIVE_RETENTION_DAYS


def _policy_dir(storage_dir: Path | None = None) -> Path:
    root = Path(storage_dir) if storage_dir else load_settings().data_dir
    return root / "web_settings"


def _policy_path(storage_dir: Path | None = None) -> Path:
    return _policy_dir(storage_dir) / "storage_policy.json"


def load_storage_policy(storage_dir: Path | None = None) -> StoragePolicyModel:
    path = _policy_path(storage_dir)
    if not path.exists():
        return StoragePolicyModel()
    payload = json.loads(path.read_text(encoding="utf-8"))
    return StoragePolicyModel(archive_retention_days=_normalize_days(payload.get("archive_retention_days", DEFAULT_ARCHIVE_RETENTION_DAYS)))


def save_storage_policy(policy: StoragePolicyModel, storage_dir: Path | None = None) -> None:
    path = _policy_path(storage_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(policy), ensure_ascii=False, indent=2), encoding="utf-8")


def update_storage_policy(*, archive_retention_days: int, storage_dir: Path | None = None) -> StoragePolicyModel:
    policy = StoragePolicyModel(archive_retention_days=_normalize_days(archive_retention_days))
    save_storage_policy(policy, storage_dir)
    return policy


def cleanup_expired_storage() -> dict[str, int]:
    policy = load_storage_policy()
    from web.history import cleanup_expired_history_artifacts
    from web.uploads import cleanup_expired_upload_storage

    upload_summary = cleanup_expired_upload_storage(retention_days=policy.archive_retention_days)
    history_summary = cleanup_expired_history_artifacts(retention_days=policy.archive_retention_days)
    return {
        "archive_retention_days": policy.archive_retention_days,
        **upload_summary,
        **history_summary,
    }


def cleanup_expired_storage_if_due(*, interval_seconds: int = DEFAULT_CLEANUP_INTERVAL_SECONDS) -> dict[str, int] | None:
    global _last_cleanup_at
    now = monotonic()
    if _last_cleanup_at and now - _last_cleanup_at < interval_seconds:
        return None
    _last_cleanup_at = now
    return cleanup_expired_storage()


def _normalize_days(value: int | str | None) -> int:
    try:
        days = int(value)
    except (TypeError, ValueError):
        days = DEFAULT_ARCHIVE_RETENTION_DAYS
    return max(1, days)
