import yaml
from fastapi import FastAPI
from pathlib import Path

from app.core.lifecycle import reload_application_state


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
