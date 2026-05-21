from datetime import datetime

from analytics.protocol_scenarios import (
    create_protocol_scenario,
    delete_protocol_scenario,
    load_protocol_scenarios,
    reset_protocol_scenarios,
    save_protocol_scenarios,
    run_protocol_scenarios,
    update_protocol_scenario,
)
from core.models import PaymentEvent, ProtocolScenario, ProtocolScenarioStep
from tests.test_counters import make_event
from tests.test_repeats import _with_event_position


def test_protocol_scenarios_use_builtin_default_when_storage_is_missing(tmp_path):
    path = tmp_path / "protocol_scenarios.json"

    scenarios = load_protocol_scenarios(path)

    assert scenarios
    assert len(scenarios) >= 9
    assert [scenario.scenario_id for scenario in scenarios[:9]] == [
        "payment_start_followup_chain",
        "info_command",
        "payment_confirm_command",
        "payment_cancel_command",
        "init_update_command",
        "get_output_command",
        "update_configuration_command",
        "set_time_command",
        "pass_through_command",
    ]
    assert scenarios[0].steps[0].event_type == "PaymentStart resp"
    assert scenarios[0].steps[1].kind == "branch"
    assert scenarios[0].steps[1].next_event_type == "PaymentConfirm"
    assert scenarios[0].source_document.endswith(".docx")
    assert scenarios[0].source_section == "PaymentStart"
    assert scenarios[0].source_sections == ["PaymentStart"]
    assert scenarios[0].source_quote.startswith("PaymentStart.")
    assert scenarios[0].source_quotes == ["PaymentStart. Таймаут по умолчанию 3 секунды."]
    assert scenarios[1].title == "Сценарий Info"
    assert scenarios[1].steps[0].raw_contains == "Info"
    assert scenarios[2].title == "Сценарий PaymentConfirm"
    assert scenarios[3].title == "Сценарий PaymentCancel"
    assert scenarios[4].title == "Сценарий InitUpdate"
    assert scenarios[5].title == "Сценарий GetOutput"
    assert scenarios[6].title == "Сценарий UpdateConfiguration"
    assert scenarios[7].title == "Сценарий SetTime"
    assert scenarios[8].title == "Сценарий PassThrough"


def test_protocol_scenarios_merge_builtin_defaults_into_existing_catalog(tmp_path):
    path = tmp_path / "protocol_scenarios.json"
    custom = ProtocolScenario(
        scenario_id="custom_only",
        title="Custom only",
        description="custom",
        enabled=True,
        version="1",
        source_document="docx",
        source_section="Custom",
        source_sections=["Custom"],
        source_quotes=[],
        source_quote="",
        steps=[
            ProtocolScenarioStep(
                kind="match",
                label="Find custom",
                raw_contains="Custom",
                source_section="Custom",
            )
        ],
    )
    save_protocol_scenarios([custom], storage_path=path)

    scenarios = load_protocol_scenarios(path)

    assert scenarios[0].scenario_id == "payment_start_followup_chain"
    assert scenarios[1].scenario_id == "info_command"
    assert any(scenario.scenario_id == "custom_only" for scenario in scenarios)
    assert len(scenarios) >= 10


def test_protocol_scenario_catalog_can_create_update_delete_and_reset(tmp_path):
    path = tmp_path / "protocol_scenarios.json"

    created = create_protocol_scenario(
        title="My scenario",
        description="custom from test",
        steps='[{"kind":"match","label":"Step 1","event_type":"PaymentStart resp","source_section":"PaymentStart"},{"kind":"branch","label":"Step 2","code_eq":0,"next_event_type":"PaymentConfirm","source_section":"PaymentStart"}]',
        source_document="docx",
        source_sections="PaymentStart\nPaymentStart resp",
        source_quotes="PaymentStart. Таймаут по умолчанию 3 секунды.\nPaymentStart resp. Ожидается переход в следующую команду.",
        storage_path=path,
    )
    assert created.scenario_id.startswith("custom_")
    assert created.steps[0].event_type == "PaymentStart resp"
    assert created.steps[1].code_eq == 0
    assert created.source_section == "PaymentStart\nPaymentStart resp"
    assert created.source_sections == ["PaymentStart", "PaymentStart resp"]
    assert created.source_quotes == [
        "PaymentStart. Таймаут по умолчанию 3 секунды.",
        "PaymentStart resp. Ожидается переход в следующую команду.",
    ]

    updated = update_protocol_scenario(
        created.scenario_id,
        title="My scenario v2",
        description="updated",
        steps='[{"kind":"match","label":"Step A","event_type":"PaymentStart resp","source_section":"PaymentStart"},{"kind":"branch","label":"Step B","code_ne":0,"next_event_type":"PaymentCancel","source_section":"PaymentStart"}]',
        source_document="docx-2",
        source_sections="PaymentStart\nPaymentStart resp",
        source_quotes="PaymentStart. Таймаут по умолчанию 3 секунды.\nPaymentStart resp. Ожидается переход в следующую команду.",
        enabled=False,
        storage_path=path,
    )
    assert updated.title == "My scenario v2"
    assert updated.description == "updated"
    assert updated.enabled is False
    assert updated.version == "2"
    assert updated.steps[1].next_event_type == "PaymentCancel"
    assert updated.source_section == "PaymentStart\nPaymentStart resp"
    assert updated.source_sections == ["PaymentStart", "PaymentStart resp"]
    assert updated.source_quote.startswith("PaymentStart.")
    assert len(updated.source_quotes) == 2

    delete_protocol_scenario(updated.scenario_id, storage_path=path)
    remaining = load_protocol_scenarios(path)
    assert remaining
    assert remaining[0].scenario_id == "payment_start_followup_chain"

    reset_protocol_scenarios(path)
    assert not path.exists()


def test_protocol_scenario_evaluator_matches_followup_chain():
    events = [
        _with_event_position(make_event(0, message="OK"), "a.log", 10, datetime(2026, 4, 29, 10, 0, 0)),
        _with_event_position(
            PaymentEvent(
                source_file="a.log",
                line_number=11,
                timestamp=None,
                event_type="PaymentConfirm",
                code=0,
                message="confirm",
                duration_ms=None,
                package=None,
                raw_line="PaymentConfirm",
            ),
            "a.log",
            11,
            datetime(2026, 4, 29, 10, 0, 1),
        ),
    ]

    results = run_protocol_scenarios(events)

    assert results
    assert results[0].scenario_id == "payment_start_followup_chain"
    assert results[0].status == "matched"
    assert "PaymentStart resp" in results[0].evidence
    assert results[0].source_document.endswith(".docx")
    assert results[0].source_quote.startswith("PaymentStart.")
    assert results[0].source_quotes == ["PaymentStart. Таймаут по умолчанию 3 секунды."]


def test_protocol_scenario_evaluator_supports_message_and_section_conditions():
    events = [
        _with_event_position(make_event(6, message="Приложите одну карту"), "a.log", 20, datetime(2026, 4, 29, 10, 0, 0)),
    ]
    custom = ProtocolScenario(
        scenario_id="message_condition",
        title="Message condition",
        description="message and section",
        enabled=True,
        version="1",
        source_document="docx",
        source_section="PaymentStart",
        source_sections=["PaymentStart", "PaymentStart resp"],
        source_quotes=["PaymentStart. Таймаут по умолчанию 3 секунды.", "PaymentStart resp. Ожидается переход в следующую команду."],
        source_quote="PaymentStart. Таймаут по умолчанию 3 секунды.\nPaymentStart resp. Ожидается переход в следующую команду.",
        steps=[
            ProtocolScenarioStep(
                kind="match",
                label="Match code 6 and message",
                event_type="PaymentStart resp",
                message_contains="карту",
                source_section="PaymentStart",
                code_eq=6,
            )
        ],
    )

    results = run_protocol_scenarios(events, scenarios=[custom])

    assert len(results) == 1
    assert results[0].status == "matched"
    assert results[0].source_section == "PaymentStart"
    assert results[0].source_sections == ["PaymentStart", "PaymentStart resp"]
    assert results[0].source_quote.startswith("PaymentStart.")
    assert len(results[0].source_quotes) == 2
