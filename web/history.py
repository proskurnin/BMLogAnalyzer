from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import json
import secrets
import shutil
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


def _history_urls(run_id: str, report_path: str = "") -> tuple[str, str]:
    if not report_path:
        return "", ""
    return f"/report/{run_id}", f"/report/{run_id}/manifest"


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


def history_summary_from_snapshot(
    snapshot: SnapshotModel,
    *,
    run_id: str,
    created_at: str,
    mode: str,
    source: str,
    report_path: str = "",
) -> HistoryItemModel:
    report_url, manifest_url = _history_urls(run_id, report_path)
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
        report_path=report_path,
        report_url=report_url,
        manifest_url=manifest_url,
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
    root.mkdir(parents=True, exist_ok=True)
    (root / "runs").mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": run_id,
        "created_at": created_at,
        "mode": mode,
        "source": source,
        "report_path": str(report_path) if report_path else "",
        "report_url": f"/report/{run_id}" if report_path else "",
        "manifest_url": f"/report/{run_id}/manifest" if report_path else "",
        "snapshot": snapshot_to_payload(snapshot),
    }
    _run_path(run_id, root).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    summary = history_summary_from_snapshot(
        snapshot,
        run_id=run_id,
        created_at=created_at,
        mode=mode,
        source=source,
        report_path=str(report_path) if report_path else "",
    )
    with _index_path(root).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(asdict(summary), ensure_ascii=False) + "\n")
    return summary


def list_history(
    storage_dir: Path | None = None,
    *,
    limit: int = 12,
    mode: str | None = None,
    query: str | None = None,
    sort: str = "desc",
) -> list[HistoryItemModel]:
    index = _index_path(storage_dir)
    if not index.exists():
        return []
    items: list[HistoryItemModel] = []
    mode_filter = (mode or "").strip().lower() or None
    query_filter = (query or "").strip().lower() or None
    for line in index.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        payload.setdefault("report_path", "")
        payload.setdefault("report_url", "")
        payload.setdefault("manifest_url", "")
        if mode_filter and str(payload.get("mode", "")).strip().lower() != mode_filter:
            continue
        if query_filter:
            searchable = " ".join(
                str(payload.get(field, ""))
                for field in ("run_id", "created_at", "mode", "source", "version", "input_path", "reports_dir")
            ).lower()
            if query_filter not in searchable:
                continue
        items.append(HistoryItemModel(**payload))
    reverse = str(sort).strip().lower() != "asc"
    items.sort(key=lambda item: (item.created_at, item.run_id), reverse=reverse)
    return items[:limit]


def latest_history(
    storage_dir: Path | None = None,
    *,
    mode: str | None = None,
    query: str | None = None,
    sort: str = "desc",
) -> HistoryItemModel | None:
    items = list_history(storage_dir, limit=1, mode=mode, query=query, sort=sort)
    return items[0] if items else None


def load_history_run(run_id: str, storage_dir: Path | None = None) -> dict[str, Any]:
    path = _run_path(run_id, storage_dir)
    if not path.exists():
        raise FileNotFoundError(run_id)
    payload = json.loads(path.read_text(encoding="utf-8"))
    report_path = payload.get("report_path") or ""
    payload.setdefault("report_url", f"/report/{run_id}" if report_path else "")
    payload.setdefault("manifest_url", f"/report/{run_id}/manifest" if report_path else "")
    return payload


def delete_history_run(run_id: str, storage_dir: Path | None = None) -> bool:
    root = _history_root(storage_dir)
    run_path = _run_path(run_id, root)
    if not run_path.exists():
        return False
    run_payload = json.loads(run_path.read_text(encoding="utf-8"))
    report_path = run_payload.get("report_path") or ""
    if report_path:
        report_file = Path(report_path)
        if report_file.exists():
            report_file.unlink()
        manifest_file = report_file.with_suffix(".json")
        if manifest_file.exists():
            manifest_file.unlink()
        if report_file.parent.exists():
            shutil.rmtree(report_file.parent, ignore_errors=True)
    run_path.unlink(missing_ok=True)
    index = _index_path(root)
    if index.exists():
        lines = [line for line in index.read_text(encoding="utf-8").splitlines() if line.strip()]
        filtered = [line for line in lines if json.loads(line).get("run_id") != run_id]
        index.write_text("\n".join(filtered) + ("\n" if filtered else ""), encoding="utf-8")
    return True
