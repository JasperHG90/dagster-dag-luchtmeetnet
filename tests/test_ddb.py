# import os
# from typing import Any, Dict, List

# import pandas as pd
# from dagster import (
#     AssetExecutionContext,
#     AssetIn,
#     DailyPartitionsDefinition,
#     asset,
#     materialize,
# )
# from dagster_aws.s3 import S3PickleIOManager, S3Resource

# resources = {
#     "io_manager": S3PickleIOManager(
#         s3_resource=S3Resource(
#             endpoint_url=os.environ["DAGSTER_SECRET_S3_ENDPOINT"],
#             aws_access_key_id=os.environ["DAGSTER_SECRET_S3_ACCESS_KEY_ID"],
#             aws_secret_access_key=os.environ["DAGSTER_SECRET_S3_SECRET_ACCESS_KEY"],
#             aws_session_token=None,
#             verify=False,
#         ),
#         s3_bucket="landingzone",
#     )
# }


# @asset(
#     partitions_def=DailyPartitionsDefinition(
#         start_date="2021-01-01", end_date="2021-01-10"
#     ),
#     io_manager_key="io_manager",
# )
# def air_quality_data(context: AssetExecutionContext) -> List[Dict[str, Any]]:
#     return pd.DataFrame.from_dict(
#         {
#             "partition": [context.partition_key] * 3,
#             "station_number": [1, 2, 3],
#             "measurement_date": ["2021-01-01", "2021-01-02", "2021-01-03"],
#             "pm10": [20, 30, 40],
#             "pm25": [10, 20, 30],
#         }
#     ).to_dict(orient="records")


# @asset(
#     io_manager_key="io_manager",
#     ins={
#         "air_quality_data": AssetIn(
#             key="air_quality_data",
#             metadata={"allow_missing_partitions": True},
#             dagster_type=dict,
#         )
#     },
# )
# def daily_air_quality_data(
#     air_quality_data: Dict[str, List[Dict[str, Any]]]
# ) -> pd.DataFrame:
#     return pd.concat(
#         [pd.DataFrame(data_partition) for data_partition in air_quality_data.values()]
#     )


# def test_duckdb():
#     for pk in ["2021-01-01", "2021-01-02", "2021-01-03"]:
#         res = materialize(
#             assets=[air_quality_data],
#             resources=resources,
#             partition_key=pk,
#         )
#         assert res.success
#     materialize(
#         assets=[air_quality_data, daily_air_quality_data],
#         resources=resources,
#         selection=[daily_air_quality_data],
#     )
