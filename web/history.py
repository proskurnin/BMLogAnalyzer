from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import json
import secrets
from pathlib import Path
from typing import Any

from web.models import HistoryItemModel, SnapshotModel


def _history_root(storage_dir: Path | None = None) -> Path:
    root = Path(storage_dir or "./_workdir/web_history")
    root.mkdir(parents=True, exist_ok=True)
    (root / "runs").mkdir(parents=True, exist_ok=True)
    return root


def _index_path(storage_dir: Path | None = None) -> Path:
    return _history_root(storage_dir) / "index.jsonl"


def _run_path(run_id: str, storage_dir: Path | None = None) -> Path:
    return _history_root(storage_dir) / "runs" / f"{run_id}.json"


def run_directory(run_id: str, storage_dir: Path | None = None) -> Path:
    directory = _history_root(storage_dir) / "runs" / run_id
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def run_report_path(run_id: str, storage_dir: Path | None = None) -> Path:
    return run_directory(run_id, storage_dir) / "report.html"


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + secrets.token_hex(4)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def snapshot_to_payload(snapshot: SnapshotModel) -> dict[str, Any]:
    return asdict(snapshot)


def history_summary_from_snapshot(snapshot: SnapshotModel, *, run_id: str, created_at: str, mode: str, source: str) -> HistoryItemModel:
    return HistoryItemModel(
        run_id=run_id,
        created_at=created_at,
        mode=mode,
        source=source,
        version=snapshot.version,
        input_path=snapshot.request.input_path,
        reports_dir=snapshot.request.reports_dir,
        total=snapshot.analysis.total,
        success_count=snapshot.analysis.success_count,
        decline_count=snapshot.analysis.decline_count,
        technical_error_count=snapshot.analysis.technical_error_count,
        unknown_count=snapshot.analysis.unknown_count,
        bm_logs=snapshot.archives.bm_logs,
        reader_logs=snapshot.archives.reader_logs,
        system_logs=snapshot.archives.system_logs,
        report_path="",
    )


def record_history(
    snapshot: SnapshotModel,
    *,
    mode: str,
    source: str,
    storage_dir: Path | None = None,
    run_id: str | None = None,
    created_at: str | None = None,
    report_path: Path | None = None,
) -> HistoryItemModel:
    created_at = created_at or _utc_now()
    run_id = run_id or new_run_id()
    root = _history_root(storage_dir)
    payload = {
        "run_id": run_id,
        "created_at": created_at,
        "mode": mode,
        "source": source,
        "report_path": str(report_path) if report_path else "",
        "snapshot": snapshot_to_payload(snapshot),
    }
    _run_path(run_id, root).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    summary = history_summary_from_snapshot(snapshot, run_id=run_id, created_at=created_at, mode=mode, source=source)
    with _index_path(root).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(asdict(summary), ensure_ascii=False) + "\n")
    return summary


def list_history(storage_dir: Path | None = None, *, limit: int = 12) -> list[HistoryItemModel]:
    index = _index_path(storage_dir)
    if not index.exists():
        return []
    items: list[HistoryItemModel] = []
    for line in index.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        payload.setdefault("report_path", "")
        items.append(HistoryItemModel(**payload))
    return items[-limit:][::-1]


def latest_history(storage_dir: Path | None = None) -> HistoryItemModel | None:
    items = list_history(storage_dir, limit=1)
    return items[0] if items else None


def load_history_run(run_id: str, storage_dir: Path | None = None) -> dict[str, Any]:
    path = _run_path(run_id, storage_dir)
    if not path.exists():
        raise FileNotFoundError(run_id)
    return json.loads(path.read_text(encoding="utf-8"))
