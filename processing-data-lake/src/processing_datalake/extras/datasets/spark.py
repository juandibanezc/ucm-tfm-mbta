"""``AbstractDataSet`` implementation to access Spark dataframes using
``pyspark`` on Apache Hive.
"""
import json
import pickle
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any, Dict

import fsspec
from kedro_datasets.spark.spark_dataset import (
    _split_filepath,
)
from kedro.io.core import (
    AbstractDataset,
    DatasetError,
    get_filepath_str,
    get_protocol_and_path,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException


from processing_datalake.hooks import (
    get_context as get_kedro_context,
)


class SparkTableDataset(AbstractDataset[DataFrame, DataFrame]):
    """``SparkTableDataSet`` loads and saves Spark dataframes.
    This dataset is able to load tables from Hive and save them back to Hive.

    This DataSet has some key assumptions:

    - Tables are identified by database and table name if the file
        path contains does not contain a slash

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog.html#use-the-data-catalog-with-the-yaml-api>`_:

    .. code-block:: yaml

        spark_dataset:
          type: processing_datalake.extras.datasets.spark.SparkTableDataSet
          directory: hive_database
          table: table_name
          save_args:
            mode: overwrite
    """

    _SINGLE_PROCESS = True
    DEFAULT_SAVE_ARGS = {}  # type: Dict[str, Any]
    DEFAULT_LOAD_ARGS = {}  # type: Dict[str, Any]

    def __init__(
        self,
        directory: str,
        table: str,
        table_format: str = "delta",
        dataset_type: str = None,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        metadata: Dict[str, Any] = None,
    ) -> None:
        """Creates a new instance of ``SparkHiveDataSet``.

        Args:
            directory: Base path to a folder or a database.
            table: Name of the table or file.
            table_format: File format used during load and save
                operations. These are formats supported by the running
                SparkContext include parquet, csv, delta. For a list of supported
                formats please refer to Apache Spark documentation at
                https://spark.apache.org/docs/latest/sql-programming-guide.html
            dataset_type: Type of the dataset. Can be either "table" or "file".
            load_args: Load args passed to Spark DataFrameReader load method.
                It is dependent on the selected file format. You can find
                a list of read options for each supported format
                in Spark DataFrame read documentation:
                https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html
            save_args: Optional mapping of any options,
                passed to the `DataFrameWriter.saveAsTable` as kwargs.
                Key example of this is `partitionBy` which allows data partitioning
                on a list of column names.
                Other `HiveOptions` can be found here:
                https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html#specifying-storage-format-for-hive-tables
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        Raises:
            DataSetError: Invalid configuration supplied
        """
        self.metadata = metadata
        if not dataset_type and ("/" in directory or "/" in table):
            self._type = "file"
        elif not dataset_type and "." in directory:
            self._type = "table"
        else:
            self._type = dataset_type
        if not self._type:
            raise DatasetError(
                "Could not determine dataset type. Please specify it explicitly."
            )

        if self._type == "table":
            directory = directory.rstrip(".")
            table = table.lstrip(".")
            path = f"{directory}.{table}"
            self._database = ".".join(path.split(".")[:-1])
            self._table = table
            self._full_table_address = f"{self._database}.{self._table}"
        else:
            directory = directory.rstrip("/")
            table = table.lstrip("/")
            path = (
                f"{directory}.{table}"
                if table_format == "deltasharing"
                else f"{directory}/{table}"
            )
            fs_prefix, filepath = _split_filepath(path)
            self._fs_prefix = fs_prefix
            self._filepath = PurePosixPath(filepath)
            self._type = "file"

        # Handle default load and save arguments
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        self._supported_modes = {
            "append",
            "overwrite",
            "error",
            "errorifexists",
            "ignore",
        }
        self._write_mode = self._save_args.pop("mode", "errorifexists")
        self._write_mode = (
            "error" if table_format == "deltasharing" else self._write_mode
        )

        # Handle schema load argument
        self._schema = self._load_args.pop("schema", None)
        if self._type == "file" and self._schema is not None:
            if isinstance(self._schema, dict):
                self._schema = self._load_schema_from_file(self._schema)

        self._file_format = table_format
        self._handle_write_mode()

    @staticmethod
    def _load_schema_from_file(schema: Dict[str, Any]) -> StructType:

        filepath = schema.get("filepath")
        if not filepath:
            raise DatasetError(
                "Schema load argument does not specify a 'filepath' attribute. Please"
                "include a path to a JSON-serialised 'pyspark.sql.types.StructType'."
            )

        credentials = deepcopy(schema.get("credentials")) or {}
        protocol, schema_path = get_protocol_and_path(filepath)
        file_system = fsspec.filesystem(protocol, **credentials)
        pure_posix_path = PurePosixPath(schema_path)
        load_path = get_filepath_str(pure_posix_path, protocol)

        # Open schema file
        with file_system.open(load_path) as fs_file:

            try:
                return StructType.fromJson(json.loads(fs_file.read()))
            except Exception as exc:
                raise DatasetError(
                    f"Contents of 'schema.filepath' ({schema_path}) are invalid. Please"
                    f"provide a valid JSON-serialised 'pyspark.sql.types.StructType'."
                ) from exc

    def _describe(self) -> Dict[str, Any]:
        if self._type == "file":
            return {
                "filepath": self._fs_prefix + str(self._filepath),
                "file_format": self._file_format,
                "load_args": self._load_args,
                "save_args": self._save_args,
            }

        return {
            "database": self._database,
            "table": self._table,
            "partition": self._save_args.get("partitionBy"),
            "format": self._file_format,
            "load_args": self._load_args,
            "save_args": self._save_args,
        }

    @staticmethod
    def _get_spark() -> SparkSession:
        """Get the active Spark session."""
        if hasattr(get_kedro_context(), "spark_session"):
            return get_kedro_context().spark_session
        else:
            SparkSession.getActiveSession()

    def _create_hive_table(self, data: DataFrame, mode: str = None):
        _mode: str = mode or self._write_mode
        data.write.saveAsTable(
            self._full_table_address,
            mode=_mode,
            format=self._file_format,
            **self._save_args,
        )

    def _load(self) -> DataFrame:
        read_obj = self._get_spark().read
        if self._type == "file":
            load_path = self._fs_prefix + str(self._filepath)
            # Pass schema if defined
            if self._schema:
                read_obj = read_obj.schema(self._schema)

            return read_obj.load(load_path, self._file_format, **self._load_args)

        return read_obj.table(self._full_table_address)

    def _save(self, data: DataFrame) -> None:
        if self._type == "file":
            save_path = self._fs_prefix + str(self._filepath)
            data.write.save(
                save_path, self._file_format, mode=self._write_mode, **self._save_args
            )
        else:
            self._validate_save(data)
            self._create_hive_table(data=data)

    def _validate_save(self, data: DataFrame):
        # do not validate when the table doesn't exist
        # or if the `write_mode` is set to overwrite
        if (
            (self._type != "table")
            or (self._write_mode == "overwrite")
            or ("mergeSchema" in self._save_args)
            or (not self._exists())
        ):
            return
        hive_dtypes = set(self._load().dtypes)
        data_dtypes = set(data.dtypes)
        if data_dtypes != hive_dtypes:
            new_cols = data_dtypes - hive_dtypes
            missing_cols = hive_dtypes - data_dtypes
            raise DatasetError(
                f"Dataset does not match hive table schema.\n"
                f"Present on insert only: {sorted(new_cols)}\n"
                f"Present on schema only: {sorted(missing_cols)}"
            )

    def _exists(self) -> bool:
        if self._type == "file":
            load_path = self._fs_prefix + str(self._filepath)

            try:
                self._get_spark().read.load(load_path, self._file_format)
            except AnalysisException as exception:
                if (
                    exception.desc.startswith("Path does not exist:")
                    or "is not a Delta table" in exception.desc
                ):
                    return False
                raise
            return True

        # noqa # pylint:disable=protected-access
        return (
            self._get_spark()
            ._jsparkSession.catalog()
            .tableExists(self._database, self._table)
        )

    def _handle_write_mode(self) -> None:
        if self._write_mode and self._write_mode not in self._supported_modes:
            raise DatasetError(
                f"It is not possible to perform 'save()' for file format "
                f"'{self._file_format}' "
                f"with mode '{self._write_mode}' on 'SparkTableDataSet'. "
            )

    def __getstate__(self) -> None:
        raise pickle.PicklingError(
            "PySpark datasets objects cannot be pickled "
            "or serialised as Python objects."
        )
