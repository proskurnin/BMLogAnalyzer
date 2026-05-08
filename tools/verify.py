from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.verification import run_healthchecks, run_readiness_check


@dataclass(frozen=True)
class CommandResult:
    name: str
    status: str
    details: str = ""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run BM Log Analyzer tests, healthchecks and readiness checks.")
    parser.add_argument("--config", default="./config/config.yaml", help="Path to YAML config file.")
    parser.add_argument("--workdir", default=None, help="Temporary workdir for readiness checks.")
    parser.add_argument("--skip-tests", action="store_true", help="Skip pytest execution.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    started = perf_counter()
    results: list[CommandResult] = []

    if not args.skip_tests:
        results.append(run_pytest())

    results.extend(CommandResult(item.name, item.status, item.details) for item in run_healthchecks(args.config))
    workdir = Path(args.workdir) if args.workdir else None
    results.extend(CommandResult(item.name, item.status, item.details) for item in run_readiness_check(workdir))

    failed = False
    for result in results:
        level = "OK" if result.status == "ok" else "FAIL"
        print(f"[VERIFY] {level} {result.name} {result.details}".rstrip(), flush=True)
        failed = failed or result.status != "ok"

    duration_ms = (perf_counter() - started) * 1000
    print(f"[VERIFY] DONE total_time={_format_duration(duration_ms)}", flush=True)
    return 1 if failed else 0


def run_pytest() -> CommandResult:
    started = perf_counter()
    process = subprocess.run([sys.executable, "-m", "pytest", "-q"], check=False, capture_output=True, text=True)
    duration_ms = (perf_counter() - started) * 1000
    details = f"time={_format_duration(duration_ms)} rc={process.returncode}"
    if process.stdout:
        details += f" stdout={_flatten(process.stdout)}"
    if process.stderr:
        details += f" stderr={_flatten(process.stderr)}"
    if process.returncode != 0:
        return CommandResult(name="pytest", status="failed", details=details)
    return CommandResult(name="pytest", status="ok", details=details)


def _flatten(text: str) -> str:
    return " ".join(line.strip() for line in text.splitlines() if line.strip())


def _format_duration(duration_ms: float) -> str:
    if duration_ms >= 1000:
        return f"{duration_ms / 1000:.2f}s"
    return f"{duration_ms:.1f}ms"


if __name__ == "__main__":
    raise SystemExit(main())
