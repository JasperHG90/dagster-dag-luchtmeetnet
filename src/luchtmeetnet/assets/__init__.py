import warnings
from typing import Any, Dict, List

import polars as pl
from dagster import (
    AllPartitionMapping,
    AssetExecutionContext,
    AssetIn,
    Backoff,
    ExperimentalWarning,
    Jitter,
    MultiToSingleDimensionPartitionMapping,
    Output,
    RetryPolicy,
    asset,
)

from luchtmeetnet.assets.utils import get_air_quality_data_for_partition_key
from luchtmeetnet.IO import LuchtMeetNetResource
from luchtmeetnet.partitions import daily_partition, daily_station_partition

warnings.filterwarnings("ignore", category=ExperimentalWarning)


@asset(
    description="Air quality data from the Luchtmeetnet API",
    kinds={"python", "s3"},
    io_manager_key="landing_zone_io_manager",
    partitions_def=daily_station_partition,
    retry_policy=RetryPolicy(
        max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS
    ),
    code_version="v1",
    group_name="measurements",
    metadata={
        "partition_expr": {
            "daily": "measurement_date",
            "stations": "station_number",
        }
    },
)
def air_quality_data(
    context: AssetExecutionContext,
    luchtmeetnet_api: LuchtMeetNetResource,
) -> Output[List[Dict[str, Any]]]:
    df = get_air_quality_data_for_partition_key(
        context.partition_key, context, luchtmeetnet_api
    )
    return Output(df.to_dict(orient="records"))


@asset(
    description="Copy air quality data to iceberg table",
    kinds={"polars", "iceberg", "s3"},
    io_manager_key="warehouse_io_manager",
    partitions_def=daily_partition,
    ins={
        "ingested_data": AssetIn(
            "air_quality_data",
            # NB: need this to control which downstream asset partitions are materialized
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="daily"
            ),
            input_manager_key="landing_zone_io_manager",
            # NB: Some partitions can fail because of 500 errors from API
            #  So we need to allow missing partitions
            metadata={"allow_missing_partitions": True},
        )
    },
    code_version="v1",
    group_name="measurements",
    metadata={
        "partition_expr": "measurement_date",
        "schema_update_mode": "update",
        "table_properties": {
            "write.metadata.previous-versions-max": 10,
        },
    },
)
def daily_air_quality_data(
    context: AssetExecutionContext, ingested_data: Dict[str, List[Dict[str, Any]]]
) -> pl.LazyFrame:
    context.log.info(f"Copying data for partition {context.partition_key}")
    return pl.concat(
        [pl.DataFrame(data_partition) for data_partition in ingested_data.values()]
    ).lazy()


@asset(
    description="Compute daily average of air quality measurements",
    kinds={"polars", "iceberg", "s3"},
    io_manager_key="warehouse_io_manager",
    group_name="measurements",
    ins={
        "daily_data": AssetIn(
            "daily_air_quality_data",
            partition_mapping=AllPartitionMapping(),
            input_manager_key="warehouse_io_manager",
        )
    },
)
def daily_avg_air_quality_data(
    context: AssetExecutionContext, daily_data: pl.LazyFrame
) -> pl.LazyFrame:
    return daily_data.group_by(["station_number", "measurement_date", "formula"]).agg(
        pl.col("value").mean()
    )
