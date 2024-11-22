# dagster-pyiceberg example using postgresql catalog

> [!WARNING] dagster-pyiceberg is in development
>
> The `dagster-pyiceberg` library is in development.

This repository contains an example for the [dagster-pyiceberg](https://jasperhg90.github.io/dagster-pyiceberg/) IO manager.

It is intended to be used together with these example projects:

- Postgresql backend
- Polaris backend

## The example

This example ingests measured air quality data for 99 stations in The Netherlands from the [Luchtmeetnet API](https://api-docs.luchtmeetnet.nl/).

This example contains three assets:

This example contains three assets:

- **air_quality_data**: Ingests data from the Luchtmeetnet API to a landing zone bucket. It is partitioned by station number and date. The data is written to storage using the [S3 Pickle IO manager](https://docs.dagster.io/_apidocs/libraries/dagster-aws#dagster_aws.s3.S3PickleIOManager). This asset uses a [Redis database](https://redis.io/) in combination with the [pyrate-limiter](https://pypi.org/project/pyrate-limiter/) python library to rate-limit requests to the Luchtmeetnet API (see rate limit information [here](https://api-docs.luchtmeetnet.nl/)).
- **daily_air_quality_data**: Copies the ingested data from the landing zone to the warehouse in an [Apache Iceberg](https://iceberg.apache.org/) table using [dagster-pyiceberg](https://github.com/JasperHG90/dagster-pyiceberg). It is partitioned by date.
- **daily_avg_air_quality_data**: Computes the daily average for each station, measurement date, and measure. It is not partitioned.

### Installing dependencies

Execute `just s` to install the python dependencies.
