from typing import Any, Dict, List, Optional

from coolname import generate_slug
from dagster import AssetExecutionContext, ConfigurableResource, ResourceDependency

from luchtmeetnet._luchtmeetnet import get_results_luchtmeetnet_endpoint
from luchtmeetnet.IO.resource_rate_limiter import RateLimiterResource


# Todo: change resource type so can add description/docs
#  https://docs.dagster.io/concepts/resources
class LuchtMeetNetResource(ConfigurableResource):  # type: ignore
    rate_limiter: ResourceDependency[RateLimiterResource]

    def request(
        self,
        endpoint: str,
        context: AssetExecutionContext,
        request_params: Optional[Dict[str, Any]] = None,
        retries_before_failing: int = 10,
        delay_in_seconds: int = 30,
        paginate: bool = True,
    ) -> List[Dict[str, Any]]:
        if not context.has_partition_key:
            partition_key = generate_slug(2)
        else:
            partition_key = context.partition_key
        context.log.debug("Acquiring rate limiter")
        self.rate_limiter.try_acquire(
            context=context,
            partition_key=partition_key,
            retries_before_failing=retries_before_failing,
            delay_in_seconds=delay_in_seconds,
        )
        context.log.debug(
            f"Requesting data from {endpoint} with params: {request_params}"
        )
        return get_results_luchtmeetnet_endpoint(
            endpoint=endpoint, request_params=request_params, paginate=paginate
        )
