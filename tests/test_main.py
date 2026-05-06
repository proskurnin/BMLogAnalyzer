import pytest

from core.version import format_version
from main import build_parser


def test_cli_version_flag_prints_application_version(capsys):
    parser = build_parser()

    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["--version"])

    assert exc.value.code == 0
    assert format_version() in capsys.readouterr().out
