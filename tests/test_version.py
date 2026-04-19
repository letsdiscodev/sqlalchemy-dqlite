"""Guard against drift between pyproject.toml version and __version__."""

import pathlib
import tomllib

import sqlalchemydqlite


def test_pyproject_matches_package_version() -> None:
    pyproject = pathlib.Path(__file__).resolve().parent.parent / "pyproject.toml"
    with pyproject.open("rb") as f:
        metadata = tomllib.load(f)
    assert metadata["project"]["version"] == sqlalchemydqlite.__version__
