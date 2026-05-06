from tools.bump_version import bump_version


def test_bumps_patch_minor_and_major_versions():
    assert bump_version("4.7.0", "patch") == "4.7.1"
    assert bump_version("4.7.0", "minor") == "4.8.0"
    assert bump_version("4.7.0", "major") == "5.0.0"
