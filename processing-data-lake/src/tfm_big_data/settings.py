"""Project settings. There is no need to edit this file unless you want to
change values
from the Kedro defaults. For further information, including these default
values, see
https://docs.kedro.org/en/stable/kedro_project_setup/settings.html."""
import os

# Instantiated project hooks.
from tfm_big_data.hooks import ContextHooks  # noqa: E402

# Hooks are executed in a Last-In-First-Out (LIFO) order.
HOOKS = (ContextHooks(),)

# Installed plugins for which to disable hook auto-registration.
DISABLE_HOOKS_FOR_PLUGINS = (
    "kedro-viz",
    "kedro-mlflow",
    "kedro_mlflow",
)

# Class that manages storing KedroSession data.
# from kedro.framework.session.store import BaseSessionStore
# SESSION_STORE_CLASS = BaseSessionStore
# Keyword arguments to pass to the `SESSION_STORE_CLASS` constructor.
# SESSION_STORE_ARGS = {
#     "path": "./sessions"
# }

from tfm_big_data.hooks import KedroSparkContext  # noqa: E402

CONTEXT_CLASS = KedroSparkContext  # pylint: disable=invalid-name

# Directory that holds configuration.
CONF_SOURCE = os.getenv("KEDRO_CONF", "conf")

# Class that manages how configuration is loaded.
from kedro.config import OmegaConfigLoader  # noqa: E402

CONFIG_LOADER_CLASS = OmegaConfigLoader
# Keyword arguments to pass to the `CONFIG_LOADER_CLASS` constructor.
CONFIG_LOADER_ARGS = {
    "base_env": "base",
    "default_run_env": "local",
    "config_patterns": {
        "spark": ["spark*", "spark*/**"],
    }
}

# Class that manages Kedro's library components.
# from kedro.framework.context import KedroContext
# CONTEXT_CLASS = KedroContext

# Class that manages the Data Catalog.
from kedro.io import DataCatalog  # noqa: E402

DATA_CATALOG_CLASS = DataCatalog
