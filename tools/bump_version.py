from __future__ import annotations

import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION_FILE = ROOT / "core" / "version.py"
README_FILE = ROOT / "README.md"

VERSION_RE = re.compile(r'__version__\s*=\s*"(?P<version>\d+\.\d+\.\d+)"')
README_VERSION_RE = re.compile(r"Current analyzer version: `(?P<version>\d+\.\d+\.\d+)`.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Bump BM Log Analyzer semantic version.")
    parser.add_argument("part", choices=["patch", "minor", "major"], help="Version part to bump.")
    args = parser.parse_args()

    current = read_version()
    next_version = bump_version(current, args.part)
    write_version(next_version)
    print(f"{current} -> {next_version}")
    return 0


def read_version() -> str:
    text = VERSION_FILE.read_text(encoding="utf-8")
    match = VERSION_RE.search(text)
    if not match:
        raise SystemExit(f"Cannot find __version__ in {VERSION_FILE}")
    return match.group("version")


def bump_version(version: str, part: str) -> str:
    major, minor, patch = [int(item) for item in version.split(".")]
    if part == "patch":
        patch += 1
    elif part == "minor":
        minor += 1
        patch = 0
    elif part == "major":
        major += 1
        minor = 0
        patch = 0
    else:
        raise ValueError(f"Unsupported bump part: {part}")
    return f"{major}.{minor}.{patch}"


def write_version(version: str) -> None:
    version_text = VERSION_FILE.read_text(encoding="utf-8")
    version_text, version_replacements = VERSION_RE.subn(f'__version__ = "{version}"', version_text, count=1)
    if version_replacements != 1:
        raise SystemExit(f"Cannot update __version__ in {VERSION_FILE}")
    VERSION_FILE.write_text(version_text, encoding="utf-8")

    readme_text = README_FILE.read_text(encoding="utf-8")
    readme_text, readme_replacements = README_VERSION_RE.subn(
        f"Current analyzer version: `{version}`.",
        readme_text,
        count=1,
    )
    if readme_replacements != 1:
        raise SystemExit(f"Cannot update current analyzer version in {README_FILE}")
    README_FILE.write_text(readme_text, encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
