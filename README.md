# dagster-pyiceberg example using postgresql catalog

> [!WARNING] dagster-iceberg is in development
>
> The `dagster-iceberg` library is in development.

This repository contains an example for the [dagster-iceberg](https://github.com/dagster-io/community-integrations/tree/main/libraries/dagster-iceberg) IO manager.

It is intended to be used together with these example projects:

- [dagster-pyiceberg-example-postgres](https://github.com/JasperHG90/dagster-pyiceberg-example-postgres)
- [dagster-pyiceberg-example-polaris](https://github.com/JasperHG90/dagster-pyiceberg-example-polaris)

## The example

This example ingests measured air quality data for 99 stations in The Netherlands from the [Luchtmeetnet API](https://api-docs.luchtmeetnet.nl/).

This example contains three assets:

- **air_quality_data**: Ingests data from the Luchtmeetnet API to a landing zone bucket. It is partitioned by station number and date. The data is written to storage using the [S3 Pickle IO manager](https://docs.dagster.io/_apidocs/libraries/dagster-aws#dagster_aws.s3.S3PickleIOManager). This asset uses a [Redis database](https://redis.io/) in combination with the [pyrate-limiter](https://pypi.org/project/pyrate-limiter/) python library to rate-limit requests to the Luchtmeetnet API (see rate limit information [here](https://api-docs.luchtmeetnet.nl/)).
- **daily_air_quality_data**: Copies the ingested data from the landing zone to the warehouse in an [Apache Iceberg](https://iceberg.apache.org/) table using [dagster-pyiceberg](https://github.com/JasperHG90/dagster-pyiceberg). It is partitioned by date.
- **daily_avg_air_quality_data**: Computes the daily average for each station, measurement date, and measure. It is not partitioned.

### Installing dependencies

Execute `just s` to install the python dependencies.
