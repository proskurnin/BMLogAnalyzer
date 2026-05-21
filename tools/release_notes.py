from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from tools.bump_version import bump_version, read_version

ROOT = Path(__file__).resolve().parents[1]
CHANGELOG_FILE = ROOT / "CHANGELOG.md"
VERSION_HEADER_RE = re.compile(r"^##\s+\d+\.\d+\.\d+\s+-\s+\d{4}-\d{2}-\d{2}\s*$", re.MULTILINE)


@dataclass(frozen=True)
class ReleaseNoteTemplate:
    version: str
    release_date: str
    summary: str
    stage: str
    prod: str
    version_bump: str
    changes: list[str]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create a structured release note template in CHANGELOG.md.")
    parser.add_argument("part", choices=["patch", "minor", "major"], help="Version part to bump.")
    parser.add_argument("--summary", default="TODO", help="Short release summary.")
    parser.add_argument("--stage", default="TODO", help="Stage deployment note.")
    parser.add_argument("--prod", default="TODO", help="Prod deployment note.")
    parser.add_argument(
        "--change",
        action="append",
        dest="changes",
        default=[],
        help="Release note bullet. Can be repeated.",
    )
    parser.add_argument("--date", default=date.today().isoformat(), help="Release date in YYYY-MM-DD format.")
    parser.add_argument("--changelog", type=Path, default=CHANGELOG_FILE, help="Path to CHANGELOG.md.")
    parser.add_argument(
        "--version",
        default=None,
        help="Explicit version for the new entry. Defaults to current version bumped by the selected part.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    current_version = read_version()
    next_version = args.version or bump_version(current_version, args.part)
    template = ReleaseNoteTemplate(
        version=next_version,
        release_date=args.date,
        summary=args.summary,
        stage=args.stage,
        prod=args.prod,
        version_bump=args.part,
        changes=[change.strip() for change in args.changes if change.strip()] or ["TODO"],
    )
    write_template(args.changelog, template)
    print(f"Inserted release note template for {template.version} into {args.changelog}")
    return 0


def render_template(template: ReleaseNoteTemplate) -> str:
    lines = [
        f"## {template.version} - {template.release_date}",
        "",
        f"- summary: {template.summary}",
        f"- stage: {template.stage}",
        f"- prod: {template.prod}",
        f"- version_bump: {template.version_bump}",
        "- changes:",
    ]
    lines.extend(f"  - {change}" for change in template.changes)
    return "\n".join(lines)


def write_template(changelog_file: Path, template: ReleaseNoteTemplate) -> None:
    text = changelog_file.read_text(encoding="utf-8")
    entry = render_template(template)
    match = VERSION_HEADER_RE.search(text)
    if match:
        updated = text[: match.start()] + entry + "\n\n" + text[match.start() :]
    else:
        updated = text.rstrip() + "\n\n" + entry + "\n"
    changelog_file.write_text(updated, encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
