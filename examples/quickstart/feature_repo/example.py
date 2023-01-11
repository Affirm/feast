# This is an example feature definition file

import pandas as pd
from datetime import datetime, timedelta

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    RequestSource,
    PushSource,
)
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, UnixTimestamp

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
driver_hourly_stats = FileSource(
    name="driver_hourly_stats_source",
    path="./data/driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Define an entity for the driver. You can think of entity as a primary key used to
# fetch features.
driver = Entity(name="driver", join_keys=["driver_id"])

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=300),
    schema=[
        Field(name="driver_id", dtype=Int64),
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
        Field(name="created", dtype=UnixTimestamp),
        Field(name="event_timestamp", dtype=UnixTimestamp),
    ],
    online=True,
    source=driver_hourly_stats,
    tags={},
)

driver_stats_fs = FeatureService(
    name="driver_activity", features=[driver_hourly_stats_view]
)

driver_hourly_stats_push_source = PushSource(
    name="driver_hourly_stats_push_source",
    batch_source=driver_hourly_stats,
)

driver_hourly_stats_stream_view = FeatureView(
    name="driver_hourly_stats_stream",
    entities=[driver],
    ttl=timedelta(days=300),
    schema=[
        Field(name="driver_id", dtype=Int64),
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
        Field(name="created", dtype=UnixTimestamp),
        Field(name="event_timestamp", dtype=UnixTimestamp),
    ],
    online=True,
    source=driver_hourly_stats_push_source,
    tags={"production": "True"},
    owner="affirm",
)


input_request = RequestSource(
    name="input_request",
    schema=[
        Field(name="int_val", dtype=Int64),
    ],
)


@on_demand_feature_view(  # noqa
    sources=[
        driver_hourly_stats_view,
        input_request,
    ],
    schema=[
        Field(name="output", dtype=Float64),
        Field(name="seconds_since_last_created_time", dtype=Float64),
        Field(name="days_since_last_created_time", dtype=Int64),
        Field(name="seconds_since_last_event_time", dtype=Float64),
        Field(name="days_since_last_event_time", dtype=Int64),
    ],
)
def transformed_conv_rate(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["output"] = inputs["conv_rate"] + inputs["int_val"]
    datedelta_create = pd.to_datetime(datetime.utcnow(), utc=True) - pd.to_datetime(
        inputs["created"], utc=True
    )
    datedelta_event = pd.to_datetime(datetime.utcnow(), utc=True) - pd.to_datetime(
        inputs["event_timestamp"], utc=True
    )
    df["seconds_since_last_created_time"] = datedelta_create.dt.total_seconds()
    df["days_since_last_created_time"] = datedelta_create.dt.days
    df["seconds_since_last_event_time"] = datedelta_event.dt.total_seconds()
    df["days_since_last_event_time"] = datedelta_event.dt.days
    return df


feature_service = FeatureService(  # noqa
    name="output_service",
    features=[
        driver_hourly_stats_view,
        transformed_conv_rate,
    ],
    owner="affirm",
)


@on_demand_feature_view(  # noqa
    sources=[
        driver_hourly_stats_stream_view,
        input_request,
    ],
    schema=[
        Field(name="output", dtype=Float64),
        Field(name="seconds_since_last_created_time", dtype=Float64),
        Field(name="days_since_last_created_time", dtype=Int64),
        Field(name="seconds_since_last_event_time", dtype=Float64),
        Field(name="days_since_last_event_time", dtype=Int64),
    ],
)
def transformed_conv_rate_stream(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["output"] = inputs["conv_rate"] + inputs["int_val"]
    datedelta_create = pd.to_datetime(datetime.utcnow(), utc=True) - pd.to_datetime(
        inputs["created"], utc=True
    )
    datedelta_event = pd.to_datetime(datetime.utcnow(), utc=True) - pd.to_datetime(
        inputs["event_timestamp"], utc=True
    )
    df["seconds_since_last_created_time"] = datedelta_create.dt.total_seconds()
    df["days_since_last_created_time"] = datedelta_create.dt.days
    df["seconds_since_last_event_time"] = datedelta_event.dt.total_seconds()
    df["days_since_last_event_time"] = datedelta_event.dt.days
    return df


feature_stream_service = FeatureService(  # noqa
    name="output_stream_service",
    features=[
        driver_hourly_stats_stream_view,
        transformed_conv_rate_stream,
    ],
    owner="affirm",
)


@on_demand_feature_view(  # noqa
    sources=[
        driver_hourly_stats_stream_view,
    ],
    schema=[
        Field(name="seconds_since_last_created_time", dtype=Float64),
        Field(name="days_since_last_created_time", dtype=Int64),
        Field(name="seconds_since_last_event_time", dtype=Float64),
        Field(name="days_since_last_event_time", dtype=Int64),
    ],
)
def transformed_conv_rate_norequest(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    datedelta_create = pd.to_datetime(datetime.utcnow(), utc=True) - pd.to_datetime(
        inputs["created"], utc=True
    )
    datedelta_event = pd.to_datetime(datetime.utcnow(), utc=True) - pd.to_datetime(
        inputs["event_timestamp"], utc=True
    )
    df["seconds_since_last_created_time"] = datedelta_create.dt.total_seconds()
    df["days_since_last_created_time"] = datedelta_create.dt.days
    df["seconds_since_last_event_time"] = datedelta_event.dt.total_seconds()
    df["days_since_last_event_time"] = datedelta_event.dt.days
    return df


feature_stream_service_norequest= FeatureService(  # noqa
    name="output_stream_service_norequest",
    features=[
        driver_hourly_stats_stream_view,
        transformed_conv_rate_norequest,
    ],
    owner="affirm",
)



input_request_date = RequestSource(
    name="input_request_date",
    schema=[
        Field(name="current_utcdatetime", dtype=UnixTimestamp),
    ],
)

@on_demand_feature_view(  # noqa
    sources=[
        input_request_date,
        driver_hourly_stats_stream_view,
    ],
    schema=[
        Field(name="seconds_since_last_created_time", dtype=Int64),
        Field(name="minutes_since_last_created_time", dtype=Float64),
        Field(name="days_since_last_created_time", dtype=Int64),
        Field(name="seconds_since_last_event_time", dtype=Int64),
        Field(name="minutes_since_last_event_time", dtype=Float64),
        Field(name="days_since_last_event_time", dtype=Int64),
    ],
)
def transformed_conv_rate_with_request_datetime(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    datedelta_create = inputs['current_utcdatetime'] - inputs["created"]
    datedelta_event = inputs['current_utcdatetime'] - inputs["event_timestamp"]
    df["seconds_since_last_created_time"] = datedelta_create.dt.total_seconds().astype(int)
    df["days_since_last_created_time"] = datedelta_create.dt.days.astype(int)
    df["minutes_since_last_created_time"] = datedelta_create.dt.total_seconds() / 60.
    df["seconds_since_last_event_time"] = datedelta_event.dt.total_seconds().astype(int)
    df["days_since_last_event_time"] = datedelta_event.dt.days.astype(int)
    df["minutes_since_last_event_time"] = datedelta_event.dt.total_seconds() / 60.
    return df


feature_stream_service_with_request_datetime = FeatureService(  # noqa
    name="output_stream_service_with_request_datetime",
    features=[
        driver_hourly_stats_stream_view,
        transformed_conv_rate_with_request_datetime,
    ],
    owner="affirm",
)
