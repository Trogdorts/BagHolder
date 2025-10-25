from pathlib import Path

import pytest

from app.scripts import reset_data


def test_reset_data_directory_wipes_contents_and_creates_config(tmp_path):
    data_dir = tmp_path / "data"
    old_file = data_dir / "old.txt"
    data_dir.mkdir()
    old_file.write_text("stale")

    reset_data.reset_data_directory(data_dir)

    assert data_dir.exists()
    assert not old_file.exists()
    assert (data_dir / "config.yaml").exists()


def test_resolve_data_dir_prefers_cli_argument(tmp_path, monkeypatch):
    env_dir = tmp_path / "env"
    cli_dir = tmp_path / "cli"
    env_dir.mkdir()
    cli_dir.mkdir()

    monkeypatch.setenv("BAGHOLDER_DATA", str(env_dir))

    resolved = reset_data._resolve_data_dir(str(cli_dir))
    assert resolved == cli_dir.resolve()


def test_resolve_data_dir_uses_env_when_present(tmp_path, monkeypatch):
    env_dir = tmp_path / "env"
    env_dir.mkdir()

    monkeypatch.setenv("BAGHOLDER_DATA", str(env_dir))

    resolved = reset_data._resolve_data_dir(None)
    assert resolved == env_dir.resolve()


def test_validate_target_rejects_root(tmp_path):
    with pytest.raises(ValueError):
        reset_data._validate_target(Path("/"))
