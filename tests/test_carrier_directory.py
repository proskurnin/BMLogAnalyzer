import pytest

from analytics.carrier_directory import (
    carrier_names_for_text,
    create_carrier_rule,
    delete_carrier_rule,
    load_carrier_rules,
    update_carrier_rule,
)


def test_carrier_directory_supports_regex_rules(tmp_path):
    path = tmp_path / "carrier_rules.json"

    create_carrier_rule(name="Regex Carrier", markers=r"mmv[12]-x86_64-\d+\.\d+\.\d+", match_type="regex", storage_path=path)

    assert "Regex Carrier" in carrier_names_for_text("p: mmv2-x86_64-1.1.7", load_carrier_rules(path))
    assert "Regex Carrier" not in carrier_names_for_text("p: mmv3-x86_64-1.1.7", load_carrier_rules(path))


def test_carrier_directory_updates_and_deletes_rows(tmp_path):
    path = tmp_path / "carrier_rules.json"
    create_carrier_rule(name="Carrier A", markers="aaa", storage_path=path)

    update_carrier_rule("Carrier A", name="Carrier B", markers="bbb", match_type="contains", storage_path=path)
    assert carrier_names_for_text("raw bbb marker", load_carrier_rules(path)) == ["Carrier B"]

    delete_carrier_rule("Carrier B", storage_path=path)
    assert carrier_names_for_text("raw bbb marker", load_carrier_rules(path)) == []


def test_carrier_directory_rejects_bad_regex(tmp_path):
    with pytest.raises(Exception):
        create_carrier_rule(name="Bad", markers="[", match_type="regex", storage_path=tmp_path / "carrier_rules.json")
