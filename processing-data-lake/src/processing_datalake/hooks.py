import logging
import threading

from pathlib import Path
from typing import Any, Dict, Union, List, Callable

from kedro.config import AbstractConfigLoader
from kedro.framework.hooks import hook_impl
from kedro.framework.context import KedroContext
from pluggy import PluginManager

logger = logging.getLogger(__name__)


def _set_context(context: KedroContext):
    """Helper function to set the current Kedro context.

    Args:
        context: The current Kedro context.
    """
    global _current_context
    _current_context = context


def get_context() -> KedroContext:
    """Get the current Kedro context.

    Returns:
        The current Kedro context.
    """
    return _current_context


class KedroSparkContext(KedroContext):
    """Custom kedro context which initializes the Spark Session."""

    def __init__(
        self,
        package_name: str,
        project_path: Union[str, Path],
        config_loader: AbstractConfigLoader,
        hook_manager: PluginManager,
        env: str = None,
        extra_params: Dict[str, Any] = None,
    ):
        logger.info("Creating Kedro Context")
        if extra_params and "connect_profile" in extra_params:
            self._connect_profile = extra_params.pop("connect_profile")
        else:
            self._connect_profile = None
        super().__init__(
            project_path, config_loader, env, package_name, hook_manager,
            extra_params
        )
        self._spark_session = None
        self._package_name = (
            package_name if package_name else Path(__file__).parent.name
        )

        self._init_spark()

    def _init_spark(self):
        from pyspark import SparkConf
        from pyspark.sql import SparkSession

        spark_session = SparkSession.getActiveSession()

        if not spark_session:
            logger.info("No active Spark session found. Initializing a new one.")

            # Carga las configuraciones desde tu archivo spark.yml
            # Estas son configuraciones de la APLICACIÓN, no del CLÚSTER.
            parameters = self.config_loader["spark"]
            spark_conf = SparkConf().setAll(list(parameters.items()))

            # Construye la sesión SIN especificar el master.
            # Spark elegirá 'local[*]' por defecto si no está en un clúster.
            spark_session_builder = (
                SparkSession.builder.appName(self._package_name)
                .config(conf=spark_conf)
            )

            self._spark_session = spark_session_builder.getOrCreate()
            self._spark_session.sparkContext.setLogLevel("WARN")
            logger.info("New Spark session created.")
            logger.info("Spark Web URL: %s", self._spark_session.sparkContext.uiWebUrl)
        else:
            logger.info("Reusing an existing Spark session.")
            self._spark_session = spark_session

    @property
    def spark_session(self):
        """Spark session property."""
        return self._spark_session


class SingletonMeta(type):
    """
    Implementation of a Singleton class using the metaclass method
    """

    _instances = {}
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    instance = super(SingletonMeta, cls).__call__(*args, **kwargs)  # noqa: E501
                    cls._instances[cls] = instance
        return cls._instances[cls]


class CurrentKedroContext(
    metaclass=SingletonMeta
):  # pylint: disable=too-few-public-methods
    """
    A singleton class that stores the current Kedro context.
    """

    def __init__(self, context: KedroContext = None):
        """
        Virtually private constructor.

        Args:
            context: The current Kedro context.
        """
        self._context: KedroContext = context

    @property
    def context(self) -> KedroContext:
        """Returns the current Kedro context."""
        return self._context

    @context.setter
    def context(self, context: KedroContext):
        """Sets the current Kedro context."""
        self._context = context


class ContextHooks:  # pylint: disable=too-few-public-methods
    """Context hooks."""

    def __init__(self, callbacks: List[Callable[["KedroSparkContext"], None]] = None):  # noqa: E501
        callbacks = callbacks if callbacks else []
        self._callbacks = callbacks

    @hook_impl
    def after_context_created(self, context: "KedroSparkContext") -> None:
        """Stores the current kedro context inside a singleton class.

        Args:
            context: The current Kedro context.
        """
        _ = CurrentKedroContext(context)
        _set_context(context)
        for c in self._callbacks:
            c(context)
