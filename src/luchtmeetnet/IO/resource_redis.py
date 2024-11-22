import logging
from contextlib import contextmanager
from typing import Iterator, Optional

from dagster import ConfigurableResource, InitResourceContext
from redis import ConnectionError, Redis

logger = logging.getLogger("dagster_pyiceberg_example.IO.resource_redis")


@contextmanager
def redis_connection(
    host: str, port: Optional[int], password: Optional[str], username: Optional[str]
) -> Iterator[Redis]:
    try:
        conn = Redis(
            host=host,
            port=port if port is not None else 6379,
            password=password,
            username=username,
        )
        yield conn
    except ConnectionError as e:
        logger.error(f"Failed to connect to Redis backend: {e}")
        raise e
    finally:
        conn.close()


class RedisResource(ConfigurableResource):
    host: str
    port: Optional[int] = None
    password: Optional[str] = None
    username: Optional[str] = None

    @contextmanager
    def connection(self, context: InitResourceContext) -> Iterator[Redis]:
        context.log.debug("Connecting to Redis backend")
        context.log.debug(f"Host: {self.host}")
        context.log.debug(f"Port: {self.port}")
        context.log.debug("Password: *************")
        context.log.debug(f"Username: {self.username}")
        conn: Redis
        with redis_connection(
            host=self.host,
            port=self.port,
            password=self.password,
            username=self.username,
        ) as conn:
            yield conn
