from typing import Dict, Iterator

import pytest
from dagster import FilesystemIOManager
from dagster_pyiceberg.config import IcebergCatalogConfig
from dagster_pyiceberg.io_manager.polars import IcebergPolarsIOManager
from pyiceberg.catalog import Catalog, load_catalog
from testcontainers.redis import RedisContainer

from luchtmeetnet.IO import LuchtMeetNetResource, RateLimiterResource, RedisResource

redis = RedisContainer(
    "redis/redis-stack-server:6.2.6-v17", port=6379, password="dagster"
)


@pytest.fixture(scope="function", autouse=True)
def landing_zone_path(tmp_path_factory: pytest.TempPathFactory) -> str:
    dir_ = tmp_path_factory.mktemp("landing_zone")
    return str(dir_.resolve())


@pytest.fixture(scope="function", autouse=True)
def warehouse_path(tmp_path_factory: pytest.TempPathFactory) -> str:
    dir_ = tmp_path_factory.mktemp("warehouse")
    return str(dir_.resolve())


@pytest.fixture(scope="function")
def catalog_config_properties(warehouse_path: str) -> Dict[str, str]:
    return {
        "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        "warehouse": f"file://{str(warehouse_path)}",
    }


@pytest.fixture(scope="session")
def catalog_name() -> str:
    return "default"


@pytest.fixture(scope="function")
def catalog(catalog_name: str, catalog_config_properties: Dict[str, str]) -> Catalog:
    return load_catalog(
        name=catalog_name,
        **catalog_config_properties,
    )


@pytest.fixture(scope="session")
def namespace_name() -> str:
    return "pytest"


@pytest.fixture(scope="function", autouse=True)
def namespace(catalog: Catalog, namespace_name: str) -> str:
    catalog.create_namespace(namespace_name)
    return namespace_name


@pytest.fixture(scope="function", autouse=True)
def setup(request: pytest.FixtureRequest) -> Iterator[RedisContainer]:
    redis.start()

    def remove_container():
        redis.stop()

    request.addfinalizer(remove_container)

    yield redis


@pytest.fixture(scope="function")
def luchtmeetnet_resource(setup: RedisContainer) -> LuchtMeetNetResource:
    return LuchtMeetNetResource(
        rate_limiter=RateLimiterResource(  # See https://api-docs.luchtmeetnet.nl/ for rate limits
            rate_calls=100,
            rate_minutes=5,
            bucket_key="luchtmeetnet_api",
            redis=RedisResource(
                host=setup.get_container_host_ip(),
                port=int(setup.get_exposed_port(setup.port)),
                username="default",
                password=setup.password,
            ),
        )
    )


@pytest.fixture(scope="function")
def landing_zone_io_manager(landing_zone_path: str) -> FilesystemIOManager:
    return FilesystemIOManager(
        base_dir=landing_zone_path,
    )


@pytest.fixture(scope="function")
def warehouse_io_manager(
    catalog_name: str, catalog_config_properties: Dict[str, str], namespace: str
) -> IcebergPolarsIOManager:
    return IcebergPolarsIOManager(
        name=catalog_name,
        config=IcebergCatalogConfig(
            properties=catalog_config_properties,
        ),
        schema=namespace,
    )


@pytest.fixture(scope="function")
def resources(
    landing_zone_io_manager: FilesystemIOManager,
    warehouse_io_manager: IcebergPolarsIOManager,
    luchtmeetnet_resource: LuchtMeetNetResource,
) -> Dict[str, object]:
    return {
        "landing_zone_io_manager": landing_zone_io_manager,
        "warehouse_io_manager": warehouse_io_manager,
        "luchtmeetnet_api": luchtmeetnet_resource,
    }
