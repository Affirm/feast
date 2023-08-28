import logging
import os.path
import traceback
import warnings
from enum import Enum
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from pyspark.sql import SparkSession

from feast import flags_helper
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from feast.errors import DataSourceNoNameException
from feast.infra.offline_stores.offline_utils import get_temp_entity_table_name
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.type_map import spark_to_feast_value_type
from feast.value_type import ValueType
from feast.infra.offline_stores.contrib.spark_offline_store.defs import (
    MATERIALIZATION_START_DATE_KEY,
    MATERIALIZATION_END_DATE_KEY
)

logger = logging.getLogger(__name__)


class SparkSourceFormat(Enum):
    csv = "csv"
    json = "json"
    parquet = "parquet"
    delta = "delta"
    avro = "avro"


class TimeDependentSparkSource(SparkSource):
    def __init__(
        self,
        *,
        name: Optional[str] = None,
        path_prefix: Optional[str] = None,
        increment_time: Optional[datetime] = None,
        path_suffix: Optional[str] = "",
        file_format: Optional[str] = None,
        event_timestamp_column: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        timestamp_field: Optional[str] = None,
    ):
        # If no name, use the table as the default name.
        if name is None:
            raise DataSourceNoNameException()

        assert name

        self.path_prefix = path_prefix
        self.path_suffix = path_suffix
        self.time_fmt = time_fmt

        self._validate_time_fmt_str(self.time_fmt)

        super().__init__(
            name=name,
            path=self._build_path(),
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )

    @staticmethod
    def _validate_time_fmt_str(s: str):
        pass

    def _split_path(self):
        return self.path.split('.....')

    def get_table_query_string(self, **kwargs) -> str:
        spark_session = SparkSession.getActiveSession()

        start_date = kwargs.get(MATERIALIZATION_START_DATE_KEY)
        end_date = kwargs.get(MATERIALIZATION_END_DATE_KEY)

        prefix, time_fmt, suffix = self._split_path()
        if start_date is None or end_date is None:
            path = prefix
        else:
            path =

        if spark_session is None:
            raise AssertionError("Could not find an active spark session.")
        try:
            df = spark_session.read.format(self.file_format).load(self.path)
        except Exception:
            logger.exception(
                "Spark read of file source failed.\n" + traceback.format_exc()
            )
        tmp_table_name = get_temp_entity_table_name()
        df.createOrReplaceTempView(tmp_table_name)

        return f"`{tmp_table_name}`"
