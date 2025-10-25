"""BagHolder FastAPI application package."""

__all__ = ["create_app", "app", "__version__"]

from .main import create_app, app  # noqa: E402
from .version import __version__  # noqa: E402
