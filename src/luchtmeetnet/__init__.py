import os

from dagster import Definitions, EnvVar
from dagster_aws.s3 import S3PickleIOManager, S3Resource
from dagster_pyiceberg.io_manager.polars import IcebergPolarsIOManager

from luchtmeetnet.assets import (
    air_quality_data,
    daily_air_quality_data,
    daily_avg_air_quality_data,
)
from luchtmeetnet.IO import LuchtMeetNetResource, RateLimiterResource, RedisResource

resources = {
    "landing_zone_io_manager": S3PickleIOManager(
        s3_resource=S3Resource(
            endpoint_url=os.environ["DAGSTER_SECRET_S3_ENDPOINT"],
            aws_access_key_id=os.environ["DAGSTER_SECRET_S3_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["DAGSTER_SECRET_S3_SECRET_ACCESS_KEY"],
            aws_session_token=None,
            verify=False,
        ),
        s3_bucket="landingzone",
    ),
    "warehouse_io_manager": IcebergPolarsIOManager(
        name="dagster_example_catalog",
        schema="air_quality",
        partition_spec_update_mode="update",
        schema_update_mode="update",
        db_io_manager="custom",
    ),
    "luchtmeetnet_api": LuchtMeetNetResource(
        rate_limiter=RateLimiterResource(  # See https://api-docs.luchtmeetnet.nl/ for rate limits
            rate_calls=100,
            rate_minutes=5,
            bucket_key="luchtmeetnet_api",
            redis=RedisResource(
                host=EnvVar("DAGSTER_SECRET_REDIS_HOST"),
                port=16564,
                password=EnvVar("DAGSTER_SECRET_REDIS_PASSWORD"),
                username=EnvVar("DAGSTER_SECRET_REDIS_USERNAME"),
            ),
        )
    ),
}


definition = Definitions(
    assets=[air_quality_data, daily_air_quality_data, daily_avg_air_quality_data],
    resources=resources,
)
