from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import re


@dataclass(frozen=True)
class CarrierRule:
    name: str
    markers: list[str]
    match_type: str = "contains"


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
        match_type = _normalize_match_type(row.get("match_type"))
        if name and markers:
            rules.append(CarrierRule(name=name, markers=markers, match_type=match_type))
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


def create_carrier_rule(
    *,
    name: str,
    markers: str,
    match_type: str = "contains",
    storage_path: Path | None = None,
) -> CarrierRule:
    rule = _carrier_rule_from_input(name=name, markers=markers, match_type=match_type)
    rules = load_carrier_rules(storage_path)
    if any(item.name == rule.name for item in rules):
        raise ValueError(f"carrier already exists: {rule.name}")
    rules.append(rule)
    save_carrier_rules(rules, storage_path)
    return rule


def update_carrier_rule(
    original_name: str,
    *,
    name: str,
    markers: str,
    match_type: str = "contains",
    storage_path: Path | None = None,
) -> CarrierRule:
    rule = _carrier_rule_from_input(name=name, markers=markers, match_type=match_type)
    rules = load_carrier_rules(storage_path)
    updated = []
    found = False
    for item in rules:
        if item.name != original_name:
            if item.name == rule.name:
                raise ValueError(f"carrier already exists: {rule.name}")
            updated.append(item)
            continue
        updated.append(rule)
        found = True
    if not found:
        raise ValueError(f"unknown carrier: {original_name}")
    save_carrier_rules(updated, storage_path)
    return rule


def delete_carrier_rule(name: str, storage_path: Path | None = None) -> None:
    rules = load_carrier_rules(storage_path)
    updated = [item for item in rules if item.name != name]
    if len(updated) == len(rules):
        raise ValueError(f"unknown carrier: {name}")
    save_carrier_rules(updated, storage_path)


def reset_carrier_rules(storage_path: Path | None = None) -> None:
    path = storage_path or _default_carrier_rules_path()
    if path.exists():
        path.unlink()


def carrier_names_for_text(text: str, rules: list[CarrierRule] | None = None) -> list[str]:
    names = []
    for rule in rules or load_carrier_rules():
        if _rule_matches(rule, text):
            names.append(rule.name)
    return list(dict.fromkeys(names))


def carrier_markers_for_text(text: str, rules: list[CarrierRule] | None = None) -> set[str]:
    markers = set()
    for rule in rules or load_carrier_rules():
        for marker in rule.markers:
            if _marker_matches(marker, text, rule.match_type):
                markers.add(marker)
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


def _normalize_match_type(value: object) -> str:
    match_type = str(value or "contains").strip().lower()
    return match_type if match_type in {"contains", "regex"} else "contains"


def _carrier_rule_from_input(*, name: str, markers: str, match_type: str) -> CarrierRule:
    clean_name = name.strip()
    clean_markers = _normalize_markers(markers)
    clean_match_type = _normalize_match_type(match_type)
    if not clean_name:
        raise ValueError("carrier name is required")
    if not clean_markers:
        raise ValueError("carrier markers are required")
    if clean_match_type == "regex":
        for marker in clean_markers:
            re.compile(marker, re.IGNORECASE)
    return CarrierRule(name=clean_name, markers=clean_markers, match_type=clean_match_type)


def _rule_matches(rule: CarrierRule, text: str) -> bool:
    return any(_marker_matches(marker, text, rule.match_type) for marker in rule.markers)


def _marker_matches(marker: str, text: str, match_type: str) -> bool:
    if not marker:
        return False
    if match_type == "regex":
        try:
            return re.search(marker, text, re.IGNORECASE) is not None
        except re.error:
            return False
    return marker.lower() in text.lower()
