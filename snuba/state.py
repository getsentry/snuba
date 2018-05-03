import redis
from contextlib import contextmanager
import time
import uuid

rds = redis.Redis('127.0.0.1', db=1)

@contextmanager
def rate_limit(bucket, per_second_limit=None, concurrent_limit=None):
    """
    A context manager for rate limiting that allows for limiting based on
    on a rolling-window per-second rate as well as the number of requests
    concurrently running.

    Uses a single redis sorted set per rate-limiting bucket to track both the
    concurrency and rate, the score is the query timestamp. Queries are thrown
    ahead in time when they start so we can count them as concurrent, and
    thrown back to their start time once they finish so we can count them
    towards the historical rate.

               time >>----->
    +-----------------------------+--------------------------------+
    | historical query window     | currently executing queries    |
    +-----------------------------+--------------------------------+
                                  ^
                                 now
    """
    max_query_duration_s = 60
    rate_lookback_s = 60
    bucket = 'snuba-ratelimit:{}'.format(bucket)
    query_id = uuid.uuid4()
    now = time.time()

    pipe = rds.pipeline()
    pipe.zremrangebyscore(bucket, '-inf', '({:f}'.format(now - rate_lookback_s)) #cleanup
    pipe.zadd(bucket, query_id, now + max_query_duration_s) # add query
    pipe.zcount(bucket, now - rate_lookback_s, now) # get rate
    pipe.zcount(bucket, '({:f}'.format(now), '+inf') # get concurrent
    try:
        _, _, rate, concurrent = pipe.execute()
    except:
        yield True # fail open if redis is having issues
        return

    per_second = rate / float(rate_lookback_s)
    allowed = (per_second_limit == None or per_second <= per_second_limit) and\
                 (concurrent_limit == None or concurrent <= concurrent_limit)
    try:
        yield allowed
    finally:
        try:
            if allowed:
                # return the query to its start time
                rds.zincrby(bucket, query_id, -float(max_query_duration_s))
            else:
                rds.zrem(bucket, query_id) # not allowed / not counted
        except:
            pass

def set_config(key, value):
    key = 'snuba_config:{}'.format(key)
    try:
        rds.set(key, value)
    except:
        pass

def get_config(key, default):
    key = 'snuba_config:{}'.format(key)
    try:
        result = rds.get(key)
        if result is not None:
            return result
    except:
        pass
    return default

def record_query(data):
    max_queries = 1000
    try:
        rds.pipeline()\
            .lpush('snuba_queries', data)\
            .ltrim('snuba_queries', 0, max_queries - 1)\
            .execute()
    except:
        pass

def get_queries():
    try:
        return rds.lrange('snuba_queries', 0, -1)
    except:
        return []
