import time
from contextlib import contextmanager

from dagster import (
    AssetExecutionContext,
    ConfigurableResource,
    InitResourceContext,
    ResourceDependency,
)
from pydantic import PrivateAttr
from pyrate_limiter import BucketFullException, Duration, Limiter, Rate, RedisBucket
from redis import Redis

from luchtmeetnet.IO.resource_redis import RedisResource


@contextmanager
def rate_limiter(
    rate_calls: int, rate_minutes: int, bucket_key: str, redis_conn: Redis
):
    rate = Rate(rate_calls, Duration.MINUTE * rate_minutes)
    bucket = RedisBucket.init(rates=[rate], redis=redis_conn, bucket_key=bucket_key)
    limiter = Limiter(bucket)
    try:
        yield limiter
    finally:
        ...


# See: https://docs.dagster.io/concepts/resources for referencing resources in resources
class RateLimiterResource(ConfigurableResource):
    rate_calls: int
    rate_minutes: int
    bucket_key: str
    redis: ResourceDependency[RedisResource]

    _limiter = PrivateAttr()

    # See: https://docs.dagster.io/concepts/resources#lifecycle-hooks for information about this method
    @contextmanager
    def yield_for_execution(self, context: InitResourceContext):
        with self.redis.connection(context=context) as redis_conn:
            with rate_limiter(
                self.rate_calls,
                self.rate_minutes,
                self.bucket_key,
                redis_conn=redis_conn,
            ) as limiter:
                self._limiter = limiter
                yield self

    def try_acquire(
        self,
        context: AssetExecutionContext,
        partition_key: str,
        retries_before_failing: int,
        delay_in_seconds: int,
    ) -> None:
        for retry in range(retries_before_failing):
            try:
                self._limiter.try_acquire(partition_key)
                context.log.debug(
                    f"Successfully retrieved connection for {partition_key} after {retry} tries."
                )
                return
            except BucketFullException as e:
                context.log.warning(
                    f"Rate limit exceeded for {partition_key} on try={retry}."
                )
                if retry == retries_before_failing - 1:
                    context.log.error(
                        (f"Rate limit exceeded for {partition_key} on try={retry}.")
                    )
                    raise BucketFullException() from e
                else:
                    time.sleep(delay_in_seconds)
                    continue
