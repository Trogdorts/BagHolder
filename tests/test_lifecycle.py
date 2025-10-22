import yaml
from fastapi import FastAPI

from app.core.lifecycle import reload_application_state


def test_reload_application_state_updates_config_and_templates(tmp_path):
    data_dir = tmp_path / "data"
    app = FastAPI()

    cfg = reload_application_state(app, data_dir=str(data_dir))

    assert app.state.config is cfg
    assert app.state.templates.env.globals["cfg"]["ui"]["theme"] == "dark"

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
