from __future__ import annotations

import copy
import os

import yaml
from dataclasses import dataclass, field
from typing import Any, Dict

DEFAULT_CONFIG = {
    "accounts": {
        "active": "primary",
        "entries": {
            "primary": {
                "name": "Primary account",
                "storage": ".",
            }
        },
    },
    "server": {"host": "0.0.0.0", "port": 8012, "data_folder": "data"},
    "ui": {
        "theme": "dark",
        "show_text": True,
        "show_market_value": True,
        "show_trade_count": False,
        "show_percentages": True,
        "show_weekends": False,
        "show_exclude_controls": True,
        "highlight_weekends": True,
        "market_value_fill_mode": "average",
        "auto_dark_mode": True,
        "opacity_gain": 0.7,
        "opacity_loss": 0.7,
        "grid_transparency": 0.8,
        "icon_color": "#6b7280",
        "primary_color": "#2563eb",
        "primary_hover_color": "#1d4ed8",
        "success_color": "#22c55e",
        "warning_color": "#f59e0b",
        "danger_color": "#dc2626",
        "danger_hover_color": "#b91c1c",
        "trade_badge_color": "#34d399",
        "trade_badge_text_color": "#111827",
    },
    "notes": {
        "enabled": True,
        "icon_opacity": 0.25,
        "icon_hover_opacity": 0.9,
        "icon_has_note_color": "#80cbc4",
        "autosave": True,
        "max_length": 4000,
    },
    "import": {
        "sources": ["thinkorswim"],
        "auto_recalculate": True,
        "backup_before_import": True,
        "accepted_formats": [".csv"],
        "max_upload_bytes": 25_000_000,
    },
    "view": {"default": "latest", "remember_last_view": True, "month_start_day": "monday"},
    "backup": {"enable_auto_backup": True, "retention_days": 7},
    "export": {
        "fill_empty_with_zero": True,
    },
    "trades": {
        "pnl_method": "fifo",
    },
    "diagnostics": {
        "debug_logging": False,
        "log_max_bytes": 1_048_576,
        "log_retention": 5,
    },
}

@dataclass
class AppConfig:
    raw: Dict[str, Any] = field(default_factory=lambda: copy.deepcopy(DEFAULT_CONFIG))
    path: str = ""

    @staticmethod
    def _merge_with_defaults(overrides: Dict[str, Any]) -> Dict[str, Any]:
        """Merge overrides with :data:`DEFAULT_CONFIG` recursively.

        The function ensures every key defined in ``DEFAULT_CONFIG`` is present in
        the resulting mapping while preserving any user-provided overrides and
        additional keys. Nested dictionaries are merged recursively so that
        missing values fall back to their defaults without clobbering
        user-provided nested options.
        """

        def merge(defaults: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
            merged = copy.deepcopy(updates) if isinstance(updates, dict) else {}
            for key, value in defaults.items():
                if isinstance(value, dict):
                    existing = merged.get(key)
                    if isinstance(existing, dict):
                        merged[key] = merge(value, existing)
                    else:
                        merged[key] = merge(value, {})
                else:
                    merged.setdefault(key, copy.deepcopy(value))
            return merged

        sanitized = overrides if isinstance(overrides, dict) else {}
        return merge(copy.deepcopy(DEFAULT_CONFIG), sanitized)

    @classmethod
    def load(cls, data_dir: str) -> "AppConfig":
        os.makedirs(data_dir, exist_ok=True)
        cfg_path = os.path.join(data_dir, "config.yaml")
        if not os.path.exists(cfg_path):
            with open(cfg_path, "w") as f:
                yaml.safe_dump(DEFAULT_CONFIG, f, sort_keys=False)
            return cls(raw=copy.deepcopy(DEFAULT_CONFIG), path=cfg_path)
        with open(cfg_path, "r") as f:
            loaded = yaml.safe_load(f) or {}
        merged = cls._merge_with_defaults(loaded)
        with open(cfg_path, "w") as f:
            yaml.safe_dump(merged, f, sort_keys=False)
        return cls(raw=merged, path=cfg_path)

    def save(self):
        with open(self.path, "w") as f:
            yaml.safe_dump(self.raw, f, sort_keys=False)

    def update_from_dict(self, new_config: Dict[str, Any]) -> None:
        """Replace the current configuration with ``new_config``.

        Parameters
        ----------
        new_config:
            A mapping describing the configuration values to apply. The values
            are merged with :data:`DEFAULT_CONFIG` so that missing keys fall
            back to their defaults. The resulting configuration is persisted to
            disk immediately.
        """

        if not isinstance(new_config, dict):
            raise ValueError("Configuration payload must be a mapping")
        self.raw = self._merge_with_defaults(new_config)
        self.save()

    def as_dict(self) -> Dict[str, Any]:
        """Return a deep copy of the configuration suitable for exporting."""

        return copy.deepcopy(self.raw)

    def get(self, *keys, default=None):
        d = self.raw
        for k in keys:
            d = d.get(k, {} if default is None else default)
        return d
