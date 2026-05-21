from pathlib import Path

from tools.release_notes import ReleaseNoteTemplate, render_template, write_template


def test_render_template_uses_structured_fields():
    template = ReleaseNoteTemplate(
        version="1.2.3",
        release_date="2026-05-22",
        summary="Краткая сводка",
        stage="Проверено на stage",
        prod="Опубликовано в prod",
        version_bump="patch",
        changes=["Первая правка", "Вторая правка"],
    )

    rendered = render_template(template)

    assert "## 1.2.3 - 2026-05-22" in rendered
    assert "- summary: Краткая сводка" in rendered
    assert "- stage: Проверено на stage" in rendered
    assert "- prod: Опубликовано в prod" in rendered
    assert "- version_bump: patch" in rendered
    assert "- changes:" in rendered
    assert "  - Первая правка" in rendered
    assert "  - Вторая правка" in rendered


def test_write_template_prepends_entry_before_history(tmp_path):
    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text(
        "# Changelog\n\nФормат записей:\n\n## 1.0.0 - 2026-01-01\n\n- summary: Старый релиз\n- stage: Проверено на stage\n- prod: Опубликовано в prod\n- version_bump: patch\n- changes:\n  - Старое изменение\n",
        encoding="utf-8",
    )
    template = ReleaseNoteTemplate(
        version="1.0.1",
        release_date="2026-05-22",
        summary="Новый релиз",
        stage="Проверено на stage",
        prod="Опубликовано в prod",
        version_bump="patch",
        changes=["Новое изменение"],
    )

    write_template(changelog, template)
    updated = changelog.read_text(encoding="utf-8")

    assert updated.index("## 1.0.1 - 2026-05-22") < updated.index("## 1.0.0 - 2026-01-01")
    assert "- summary: Новый релиз" in updated
    assert "  - Новое изменение" in updated
