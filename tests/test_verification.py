from __future__ import annotations

from core.verification import run_healthchecks, run_readiness_check


def test_healthchecks_report_core_bootstrap(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "input_path: ./input",
                "reports_dir: ./reports",
                "extracted_dir: ./extracted",
                "reports:",
                "  analysis_report_html: true",
            ]
        ),
        encoding="utf-8",
    )

    outcomes = run_healthchecks(config_path)
    outcome_map = {outcome.name: outcome for outcome in outcomes}

    assert outcome_map["config_load"].status == "ok"
    assert outcome_map["cli_bootstrap"].status == "ok"
    assert outcome_map["version_format"].status == "ok"


def test_readiness_check_runs_end_to_end(tmp_path):
    outcomes = run_readiness_check(tmp_path)
    outcome_map = {outcome.name: outcome for outcome in outcomes}

    assert outcome_map["analysis_smoke"].status == "ok"
    assert outcome_map["report_generation"].status == "ok"
    assert outcome_map["pipeline_steps"].status == "ok"
    assert (tmp_path / "reports" / "analysis_report.html").exists()
