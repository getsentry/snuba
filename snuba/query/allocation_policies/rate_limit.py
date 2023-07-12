from __future__ import annotations
from snuba.query.allocation_policies import (
    AllocationPolicy,
    AllocationPolicyConfig,
    QuotaAllowance,
)


DEFAULT_CONCURRENT_QUERIES_LIMIT = 22
DEFAULT_PER_SECOND_QUERIES_LIMIT = 50


def rate_limit(
    rate_limit_params: RateLimitParameters,
) -> Iterator[Optional[RateLimitStats]]:
    """
    A context manager for rate limiting that allows for limiting based on:
        * a rolling-window per-second rate
        * the number of queries concurrently running.

    It uses one redis sorted set to keep track of both of these limits
    The following mapping is kept in redis:

        bucket: SortedSet([(timestamp1, query_id1), (timestamp2, query_id2) ...])


    Queries are thrown ahead in time when they start so we can count them
    as concurrent, and thrown back to their start time once they finish so
    we can count them towards the historical rate. See the comments for
    an example.

               time >>----->
    +-----------------------------+--------------------------------+
    | historical query window     | currently executing queries    |
    +-----------------------------+--------------------------------+
                                  ^
                                 now
    """
    (rate_history_s, rate_limit_shard_factor,) = state.get_configs(
        [
            # bool (0/1) flag to disable rate limits altogether
            ("bypass_rate_limit", 0),
            # number of seconds the timestamps are kept
            ("rate_history_sec", 3600),
            # number of shards that each redis set is supposed to have.
            # increasing this value multiplies the number of redis keys by that
            # factor, and (on average) reduces the size of each redis set
            ("rate_limit_shard_factor", 1),
        ]
    )
    assert isinstance(rate_history_s, (int, float))
    assert isinstance(rate_limit_shard_factor, int)
    assert rate_limit_shard_factor > 0

    if bypass_rate_limit == 1:
        yield None
        return

    now = time.time()

    query_id_uuid = uuid.uuid4()
    query_id = str(query_id_uuid)

    # Compute the set shard to which we should add and remove the query_id
    bucket_shard = int(query_id_uuid) % rate_limit_shard_factor
    query_bucket = _get_bucket_key(
        state.ratelimit_prefix, rate_limit_params.bucket, bucket_shard
    )

    pipe = rds.pipeline(transaction=False)
    # cleanup old query timestamps past our retention window
    #
    # it is fine to only perform this cleanup for the shard of the current
    # query, because on average there will be many other queries that hit other
    # shards and perform cleanup there.
    pipe.zremrangebyscore(query_bucket, "-inf", "({:f}".format(now - rate_history_s))

    # Now for the tricky bit:
    # ======================
    # The query's *deadline* is added to the sorted set of timestamps, therefore
    # labeling its execution as in the future.

    # All queries with timestamps in the future are considered to be executing *right now*
    # Example:

    # now = 100
    # max_query_duration_s = 30
    # rate_lookback_s = 10
    # sorted_set (timestamps only for clarity) = [91, 94, 97, 103, 105, 130]

    # EXPLANATION:
    # ===========

    # queries that have finished running
    # (in this example there are 3 queries in the last 10 seconds
    #  thus the per second rate is 3/10 = 0.3)
    #      |
    #      v
    #  -----------              v--- the current query, vaulted into the future
    #  [91, 94, 97, 103, 105, 130]
    #               -------------- < - queries currently running
    #                                (how many queries are
    #                                   running concurrently; in this case 3)
    #              ^
    #              | current time
    pipe.zadd(query_bucket, {query_id: now + state.max_query_duration_s})

    # bump the expiration date of the entire set so that it roughly aligns with
    # the expiration date of the latest item.
    #
    # we do this in order to avoid leaking redis sets in the event that two
    # things occur at the same time:
    # 1. a bucket stops receiving requests (this can happen if a bucket
    #    corresponds to a deleted project id)
    # 2. a previous request to the same bucket was killed off so that the set
    #    has a dangling item (ie. a process was killed)
    #
    # the TTL is calculated as such:
    #
    # * in the previous zadd command, the last item is inserted with timestamp
    #   `now + max_query_duration_s`.
    # * the next query's zremrangebyscore would remove this item on `now +
    #   max_query_duration_s + rate_history_s` at the earliest.
    # * add +1 to account for rounding errors when casting to int
    pipe.expire(query_bucket, int(state.max_query_duration_s + rate_history_s + 1))

    if rate_limit_params.per_second_limit is not None:
        # count queries that have finished for the per-second rate
        for shard_i in range(rate_limit_shard_factor):
            bucket = _get_bucket_key(
                state.ratelimit_prefix, rate_limit_params.bucket, shard_i
            )
            pipe.zcount(bucket, now - state.rate_lookback_s, now)

    if rate_limit_params.concurrent_limit is not None:
        # count the amount queries in the "future" which tells us the amount
        # of concurrent queries
        for shard_i in range(rate_limit_shard_factor):
            bucket = _get_bucket_key(
                state.ratelimit_prefix, rate_limit_params.bucket, shard_i
            )
            pipe.zcount(bucket, "({:f}".format(now), "+inf")

    try:
        pipe_results = iter(pipe.execute())

        # skip zremrangebyscore, zadd and expire
        next(pipe_results)
        next(pipe_results)
        next(pipe_results)

        if rate_limit_params.per_second_limit is not None:
            historical = sum(next(pipe_results) for _ in range(rate_limit_shard_factor))
        else:
            historical = 0

        if rate_limit_params.concurrent_limit is not None:
            concurrent = sum(next(pipe_results) for _ in range(rate_limit_shard_factor))
        else:
            concurrent = 0
    except Exception as ex:
        logger.exception(ex)
        yield None  # fail open if redis is having issues
        return

    per_second = historical / float(state.rate_lookback_s)

    stats = RateLimitStats(rate=per_second, concurrent=concurrent)

    rate_limit_name = rate_limit_params.rate_limit_name

    Reason = namedtuple("Reason", "scope name val limit")
    reasons = [
        Reason(
            rate_limit_name,
            "concurrent",
            concurrent,
            rate_limit_params.concurrent_limit,
        ),
        Reason(
            rate_limit_name,
            "per-second",
            per_second,
            rate_limit_params.per_second_limit,
        ),
    ]
    reason = next((r for r in reasons if r.limit is not None and r.val > r.limit), None)
    if reason:
        try:
            # Remove the query from the sorted set
            # because we rate limited it. It shouldn't count towards
            # rate limiting future queries in this bucket.
            rds.zrem(query_bucket, query_id)
        except Exception as ex:
            logger.exception(ex)

        raise RateLimitExceeded(
            "{r.scope} {r.name} of {r.val:.0f} exceeds limit of {r.limit:.0f}".format(
                r=reason
            ),
            scope=reason.scope,
            name=reason.name,
        )

    rate_limited = False
    try:
        yield stats
        _, err, _ = sys.exc_info()
        if isinstance(err, RateLimitExceeded):
            # If another rate limiter throws an exception, it won't be propagated
            # through this context. So check for the exception explicitly.
            # If another rate limit was hit, we don't want to count this query
            # against this limit.
            try:
                rds.zrem(query_bucket, query_id)  # not allowed / not counted
                rate_limited = True
            except Exception as ex:
                logger.exception(ex)
    finally:
        try:
            # return the query to its start time, if the query_id was actually added.
            if not rate_limited:
                rds.zincrby(query_bucket, -float(state.max_query_duration_s), query_id)
        except Exception as ex:
            logger.exception(ex)


class RateLimitAllocationPolicy(AllocationPolicy):
    def _additional_config_definitions(self) -> list[AllocationPolicyConfig]:
        # Define policy specific config definitions, these will be used along
        # with the default definitions of the base class. (is_enforced, is_active)
        return [
            AllocationPolicyConfig(
                name="concurrent_limit",
                description="maximum amount of concurrent queries per tenant",
                value_type=int,
                default=DEFAULT_CONCURRENT_QUERIES_LIMIT,
            ),
            AllocationPolicyConfig(
                name="per_second_limit",
                description="maximum amount of concurrent queries per tenant",
                value_type=int,
                default=DEFAULT_PER_SECOND_QUERIES_LIMIT,
            ),
            AllocationPolicyConfig(
                name="limit_name",
                description="the name of the rate limit bucket e.g. project_concurrent, don't change this unless you have a really good reason",
                value_type=str,
                default="default",
            ),
            AllocationPolicyConfig(
                name="rate_history_sec",
                description="the amount of seconds timestamps are kept in redis",
                value_type=int,
                default=3600,
            ),
            AllocationPolicyConfig(
                name="rate_limit_shard_factor",
                description="""number of shards that each redis set is supposed to have.
                 increasing this value multiplies the number of redis keys by that
                 factor, and (on average) reduces the size of each redis set""",
                value_type=int,
                default=1,
            ),
        ]

        def _get_quota_allowance(
            self, tenant_ids: dict[str, str | int], query_id: str
        ) -> QuotaAllowance:
            # before a query is run on clickhouse, make a decision whether it can be run and with
            # how many threads
            pass

        def _update_quota_balance(
            self,
            tenant_ids: dict[str, str | int],
            query_id: str,
            result_or_error: QueryResultOrError,
        ) -> None:
            # after the query has been run, update whatever this allocation policy
            # keeps track of which will affect subsequent queries
            pass
