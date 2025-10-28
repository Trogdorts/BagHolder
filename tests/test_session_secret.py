import importlib
import sys
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _reload_main_module():
    import app.main as main_module

    return importlib.reload(main_module)


def test_session_secret_generated_and_persisted(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    monkeypatch.delenv("BAGHOLDER_SECRET_KEY", raising=False)

    main_module = _reload_main_module()
    app = main_module.create_app()

    security_cfg = app.state.config.raw.get("security", {})
    secret = security_cfg.get("session_secret")

    assert isinstance(secret, str) and secret

    cfg_path = Path(app.state.config.path)
    with cfg_path.open("r", encoding="utf-8") as handle:
        persisted = yaml.safe_load(handle) or {}

    assert persisted.get("security", {}).get("session_secret") == secret

    middleware = next(
        m
        for m in app.user_middleware
        if getattr(m.cls, "__name__", "") == "SignedCookieSessionMiddleware"
    )
    assert middleware.kwargs["secret_key"] == secret


def test_session_secret_reused_from_config(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("BAGHOLDER_DATA", str(data_dir))
    monkeypatch.delenv("BAGHOLDER_SECRET_KEY", raising=False)

    cfg_path = data_dir / "config.yaml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    expected_secret = "persisted-secret-value"
    with cfg_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            {
                "security": {
                    "session_secret": expected_secret,
                }
            },
            handle,
            sort_keys=False,
        )

    main_module = _reload_main_module()
    app = main_module.create_app()

    security_cfg = app.state.config.raw.get("security", {})
    assert security_cfg.get("session_secret") == expected_secret

    middleware = next(
        m
        for m in app.user_middleware
        if getattr(m.cls, "__name__", "") == "SignedCookieSessionMiddleware"
    )
    assert middleware.kwargs["secret_key"] == expected_secret
