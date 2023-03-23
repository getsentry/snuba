# Ported from Sentry

"""
The ratelimiter used by the metrics string indexer to rate-limit DB writes.

As opposed to the API rate limiter, a limit in the "sliding window" rate
limiter such as "10 requests / minute" does not reset to 0 every minute.
Instead each window can be configured with a "granularity" setting such that
the window gradually resets in steps of `granularity` seconds.

Additionally this rate-limiter is not coupled to per-project/organization
scopes, and can apply multiple sliding windows at once. On the flipside it is
not strongly consistent and depending on usage it is very easy to over-spend
quota, as checking quota and spending quota are two separate steps.

Example
=======

We want to enforce the number of requests per organization via two limits:

* 100 per 30-second
* 10 per 3-seconds

On every request, our API endpoint calls:

    check_and_use_quotas(RequestedQuota(
        prefix=f"org-id:{org_id}"
        quotas=[
            Quota(
                window_seconds=30,
                limit=100,

                # can be arbitrary depending on how "sliding" the sliding
                # window should be. This one configures per-10-second granularity
                # to make the example simpler
                granularity=10,
            ),
            Quota(
                window_seconds=3,
                limit=10,
                granularity=1,
            )
        ]
    ))

For a request happening at time `900` for `org_id=123`, the redis backend
checks the following keys::

    sliding-window-rate-limit:123:3:900
    sliding-window-rate-limit:123:3:899
    sliding-window-rate-limit:123:3:898
    sliding-window-rate-limit:123:30:90
    sliding-window-rate-limit:123:30:89
    sliding-window-rate-limit:123:30:88

...none of which exist, so the values are assumed 0 and the request goes
through. It then sets the following keys:

    sliding-window-rate-limit:123:30:90 += 1
    sliding-window-rate-limit:123:3:900 += 1

Another request for the same org happens at time `902`.

* The keys starting with `:123:30:` sum up to 1, so the 30-second limit of 100 is not exceeded.
* The keys starting with `:123:3:` sum up to 1, so the 3-second limit of 10 is not exceeded.

This request is granted the minimum allowed from the two requested, in this case that is 9 (3-second limit of 10 - 1 used)

Because no quota is exceeded, the request is granted. If one quota summed up to
100 or 10, respectively, the request would be rejected.

When using the quotas, the keys change as follows:

    sliding-window-rate-limit:123:3:900 = 1
    sliding-window-rate-limit:123:3:902 = 1
    sliding-window-rate-limit:123:30:90 = 2

"""

from collections import defaultdict
from dataclasses import dataclass
from time import time
from typing import Iterator, MutableMapping, Optional, Sequence, Tuple

from snuba.redis import RedisClientKey, get_redis_client


class InvalidConfiguration(Exception):
    pass


@dataclass(frozen=True)
class Quota:
    # The number of seconds to apply the limit to.
    window_seconds: int

    # A number between 1 and `window_seconds`. Since `window_seconds` is a
    # sliding window, configure what the granularity of that window is.
    #
    # If this is equal to `window_seconds`, the quota resets to 0 every
    # `window_seconds`.  If this is a very small number, the window slides
    # "more smoothly" at the expense of having much more redis keys.
    #
    # The number of redis keys required to enforce a quota is `window_seconds /
    # granularity_seconds`.
    granularity_seconds: int

    #: How many units are allowed within the given window.
    limit: int

    # Override the prefix given by RequestedQuota such that one can implement
    # global limits + per-organization limits. The GrantedQuota will still only
    # contain the prefix of the RequestedQuota
    prefix_override: Optional[str] = None

    def __post__init__(self) -> None:
        assert self.window_seconds % self.granularity_seconds == 0

    def iter_window(self, request_timestamp: int) -> Iterator[int]:
        """
        Iterate over the quota's window, yielding values representing each
        (absolute) granule.

        This function is used to calculate keys for storing the number of
        requests made in each granule.

        The iteration is done in reverse-order (newest timestamp to oldest),
        starting with the key to which a currently-processed request should be
        added. That request's timestamp is `request_timestamp`.

        * `request_timestamp / self.granularity_seconds - 1`
        * `request_timestamp / self.granularity_seconds - 2`
        * `request_timestamp / self.granularity_seconds - 3`
        * ...
        """

        value = request_timestamp // self.granularity_seconds

        for granule_i in range(self.window_seconds // self.granularity_seconds):
            value -= 1
            assert value >= 0, value
            yield value


@dataclass(frozen=True)
class RequestedQuota:
    # A string that all redis state is prefixed with. For example
    # `sentry-string-indexer:123` where 123 is an organization id.
    #
    # Note: You cannot control the redis sharding this way, so curly braces are
    # forbidden.
    prefix: str

    # How much of each quota's limit is requested
    requested: int

    # Which quotas to check against. The requested amount must "fit" into all
    # quotas.
    quotas: Sequence[Quota]


@dataclass(frozen=True)
class GrantedQuota:
    # The prefix from RequestedQuota
    prefix: str

    # How much of RequestedQuota.requested can actually be used.
    granted: int

    # If RequestedQuota.requested > GrantedQuota.granted, this contains the
    # quotas that were reached.
    reached_quotas: Sequence[Quota]


Timestamp = int


class RedisSlidingWindowRateLimiter:
    def __init__(self) -> None:
        self.client = get_redis_client(RedisClientKey.RATE_LIMITER)

    def validate(self) -> None:
        try:
            self.client.ping()
            self.client.connection_pool.disconnect()
        except Exception as e:
            raise InvalidConfiguration(str(e))

    def _build_redis_key_raw(
        self, prefix: str, window: int, granularity: int, granule: int
    ) -> str:
        if "{" in prefix or "}" in prefix:
            # The rate limiter currently does not allow you to control the
            # Redis sharding key through the prefix`. This is currently an
            # arbitrary limitation, but the reason for this is that one day we
            # may want to rewrite the internals to run inside of a Lua script
            # to allow for (partially) atomic check-and-use of rate limits (or
            # do that for performance reasons), in which case the rate limiter
            # would have to take control of sharding itself.
            raise ValueError("Explicit sharding not allowed in RequestedQuota.prefix")

        return f"sliding-window-rate-limit:{prefix}:{window}:{granularity}:{granule}"

    def _build_redis_key(
        self, request: RequestedQuota, quota: Quota, granule: int
    ) -> str:
        return self._build_redis_key_raw(
            prefix=quota.prefix_override or request.prefix,
            window=quota.window_seconds,
            granularity=quota.granularity_seconds,
            granule=granule,
        )

    def check_within_quotas(
        self, requests: Sequence[RequestedQuota], timestamp: Optional[Timestamp] = None
    ) -> Tuple[Timestamp, Sequence[GrantedQuota]]:
        if timestamp is None:
            timestamp = int(time())
        else:
            timestamp = int(timestamp)

        keys_to_fetch = set()
        for request in requests:
            # We could potentially run this check inside of __post__init__ of
            # RequestedQuota, but the list is actually mutable after
            # construction.
            assert request.quotas

            for quota in request.quotas:
                for granule in quota.iter_window(timestamp):
                    keys_to_fetch.add(
                        self._build_redis_key(
                            request=request, quota=quota, granule=granule
                        )
                    )

        # Stabilize the iteration order of `keys_to_fetch` by converting it
        # into a list, because the next line will iterate over keys_to_fetch
        # twice.
        #
        # While CPython 3.8 does not change the iteration order of a set as
        # long as it is not being modified
        # (https://stackoverflow.com/a/3812600/1544347), there are no formal
        # guarantees about it.
        ordered_keys_to_fetch = list(keys_to_fetch)
        p = self.client.pipeline()
        for k in ordered_keys_to_fetch:
            p.get(k)
        redis_results = dict(zip(ordered_keys_to_fetch, p.execute()))

        results = []

        # for "global quotas" (=quotas using prefix_override, which may be
        # present in multiple requests), we keep a cache of how much quota we
        # have used within this function call
        #
        # the mapping is id(Quota) -> <already granted quota>
        #
        # this prevents us from seriously overcommitting on the global quota,
        # just because each request happens to fit into it
        quota_used_cache: MutableMapping[int, int] = defaultdict(int)

        for request in requests:
            # We start out with assuming the entire request can be granted in
            # its entirety.
            granted_quota = request.requested
            reached_quotas = []

            # A request succeeds (partially) if it fits (partially) into all
            # quotas. For each quota, we calculate how much quota has been used
            # up, and trim the granted_quota by the remaining quota.
            #
            # We need to explicitly handle the possibility that quotas have
            # been overused, in those cases we want to truncate resulting
            # negative "grants" to zero.
            for quota in request.quotas:
                used_quota = (
                    sum(
                        int(
                            redis_results.get(
                                self._build_redis_key(
                                    request=request, quota=quota, granule=granule
                                )
                            )
                            or 0
                        )
                        for granule in quota.iter_window(timestamp)
                    )
                    + quota_used_cache[id(quota)]
                )

                remaining_quota = max(0, quota.limit - used_quota)

                if remaining_quota < granted_quota:
                    granted_quota = remaining_quota
                    reached_quotas.append(quota)

            for quota in request.quotas:
                if quota.prefix_override:
                    quota_used_cache[id(quota)] += granted_quota

            results.append(
                GrantedQuota(
                    prefix=request.prefix,
                    granted=granted_quota,
                    reached_quotas=reached_quotas,
                )
            )

        return timestamp, results

    def use_quotas(
        self,
        requests: Sequence[RequestedQuota],
        grants: Sequence[GrantedQuota],
        timestamp: Timestamp,
    ) -> None:
        assert len(requests) == len(grants)

        keys_to_incr: MutableMapping[str, int] = {}
        keys_ttl: MutableMapping[str, int] = {}

        for request, grant in zip(requests, grants):
            assert request.prefix == grant.prefix

            for quota in request.quotas:
                # Only incr most recent granule
                granule = next(quota.iter_window(timestamp))
                key = self._build_redis_key(
                    request=request, quota=quota, granule=granule
                )
                keys_to_incr.setdefault(key, 0)
                keys_to_incr[key] += grant.granted
                keys_ttl[key] = quota.window_seconds

        with self.client.pipeline(transaction=False) as pipeline:
            for key, value in keys_to_incr.items():
                pipeline.incrby(key, value)
                # Expire the key in `window_seconds`. Since the key has been
                # recently incremented we know it represents a current
                # timestamp. We could use expireat here, but in tests we use
                # timestamps starting from 0 for convenience.
                pipeline.expire(key, keys_ttl[key])

            pipeline.execute()

    def check_and_use_quotas(
        self, requests: Sequence[RequestedQuota], timestamp: Optional[Timestamp] = None
    ) -> Sequence[GrantedQuota]:
        """
        Check the quota requests in Redis and consume the quota in one go. See
        `check_within_quotas` for parameters.
        """
        timestamp, grants = self.check_within_quotas(requests, timestamp)
        self.use_quotas(requests, grants, timestamp)
        return grants
