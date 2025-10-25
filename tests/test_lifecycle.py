import shutil
from pathlib import Path

import yaml
from fastapi import FastAPI

from app.core.lifecycle import get_default_data_dir, reload_application_state


def test_reload_application_state_updates_config_and_templates(tmp_path):
    data_dir = tmp_path / "data"
    app = FastAPI()

    cfg = reload_application_state(app, data_dir=str(data_dir))

    assert app.state.config is cfg
    assert app.state.templates.env.globals["cfg"]["ui"]["theme"] == "dark"
    assert Path(app.state.log_path).name == "bagholder.log"
    assert Path(app.state.log_path).exists()
    assert app.state.debug_logging_enabled is False
    assert app.state.account_data_dir == str(data_dir)
    assert app.state.active_account.id == "primary"
    assert len(app.state.accounts) == 1
    accounts_global = app.state.templates.env.globals["accounts"]
    assert accounts_global and accounts_global[0]["is_active"] is True

    cfg_path = data_dir / "config.yaml"
    assert cfg_path.exists()

    with open(cfg_path, "r", encoding="utf-8") as handle:
        contents = yaml.safe_load(handle) or {}

    contents.setdefault("ui", {})["theme"] = "light"

    with open(cfg_path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(contents, handle, sort_keys=False)

    updated_cfg = reload_application_state(app, data_dir=str(data_dir))

    assert updated_cfg.raw["ui"]["theme"] == "light"
    assert app.state.templates.env.globals["cfg"]["ui"]["theme"] == "light"


def test_default_data_dir_is_under_project(monkeypatch):
    monkeypatch.delenv("BAGHOLDER_DATA", raising=False)
    default_dir = Path(get_default_data_dir())

    if default_dir.exists():
        shutil.rmtree(default_dir)

    app = FastAPI()
    reload_application_state(app)

    try:
        account_path = Path(app.state.account_data_dir).resolve()
        log_path = Path(app.state.log_path).resolve()

        try:
            account_path.relative_to(default_dir.resolve())
        except ValueError:
            raise AssertionError("Account data dir is not inside the default data directory")

        try:
            log_path.relative_to(default_dir.resolve())
        except ValueError:
            raise AssertionError("Log path is not inside the default data directory")
    finally:
        shutil.rmtree(default_dir)


def test_debug_logging_can_be_forced_via_env(tmp_path, monkeypatch):
    monkeypatch.setenv("BAGHOLDER_DEBUG_LOGGING", "true")
    data_dir = tmp_path / "data"
    app = FastAPI()

    reload_application_state(app, data_dir=str(data_dir))

    assert app.state.debug_logging_enabled is True
