from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path


@dataclass(frozen=True)
class CarrierRule:
    name: str
    markers: list[str]


DEFAULT_CARRIER_RULES = [
    CarrierRule("НБС", ["mgt_nbs"]),
    CarrierRule("АСКП", ["mgt_askp", "askp"]),
    CarrierRule("ЦППК", ["mcd"]),
    CarrierRule("МЦД-1", ["mmv1"]),
    CarrierRule("МЦД-2", ["mmv2"]),
    CarrierRule("МЦД-3", ["mmv3"]),
    CarrierRule("МЦД-4", ["mmv4"]),
    CarrierRule("ММ", ["metro", "mm-"]),
]


def load_carrier_rules(storage_path: Path | None = None) -> list[CarrierRule]:
    path = storage_path or _default_carrier_rules_path()
    if not path.exists():
        return list(DEFAULT_CARRIER_RULES)
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("carriers", payload) if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return list(DEFAULT_CARRIER_RULES)
    rules = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        markers = _normalize_markers(row.get("markers"))
        if name and markers:
            rules.append(CarrierRule(name=name, markers=markers))
    return rules or list(DEFAULT_CARRIER_RULES)


def save_carrier_rules(rules: list[CarrierRule], storage_path: Path | None = None) -> None:
    path = storage_path or _default_carrier_rules_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "bm-log-analyzer.carrier-directory.v1",
        "carriers": [asdict(rule) for rule in rules],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def update_carrier_rules(names: list[str], markers: list[str], storage_path: Path | None = None) -> list[CarrierRule]:
    rules = []
    for name, marker_text in zip(names, markers, strict=False):
        clean_name = name.strip()
        clean_markers = _normalize_markers(marker_text)
        if clean_name and clean_markers:
            rules.append(CarrierRule(name=clean_name, markers=clean_markers))
    if not rules:
        rules = list(DEFAULT_CARRIER_RULES)
    save_carrier_rules(rules, storage_path)
    return rules


def reset_carrier_rules(storage_path: Path | None = None) -> None:
    path = storage_path or _default_carrier_rules_path()
    if path.exists():
        path.unlink()


def carrier_names_for_text(text: str, rules: list[CarrierRule] | None = None) -> list[str]:
    lowered = text.lower()
    names = []
    for rule in rules or load_carrier_rules():
        if any(marker.lower() in lowered for marker in rule.markers):
            names.append(rule.name)
    return list(dict.fromkeys(names))


def carrier_markers_for_text(text: str, rules: list[CarrierRule] | None = None) -> set[str]:
    lowered = text.lower()
    markers = set()
    for rule in rules or load_carrier_rules():
        for marker in rule.markers:
            clean_marker = marker.lower()
            if clean_marker and clean_marker in lowered:
                markers.add(clean_marker)
    return markers


def _default_carrier_rules_path() -> Path:
    configured = os.getenv("BM_CARRIER_RULES_PATH", "").strip()
    if configured:
        return Path(configured)
    return Path(os.getenv("BM_DATA_DIR", "./_workdir")) / "web_settings" / "carrier_rules.json"


def _normalize_markers(value: object) -> list[str]:
    if isinstance(value, list):
        raw = ",".join(str(item) for item in value)
    else:
        raw = str(value or "")
    return [item.strip().lower() for item in raw.split(",") if item.strip()]
