from contextlib import contextmanager
import logging
import redis
import simplejson as json
import six
import time
import uuid

from snuba import settings

logger = logging.getLogger('snuba.state')

rds = redis.StrictRedis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB
)

# Window for concurrent query counting
max_query_duration_s = 60
# Window for determining query rate
rate_lookback_s = 60
# Amount of time we keep rate history
rate_history_s = 3600


ratelimit_prefix = 'snuba-ratelimit:'
query_lock_prefix = 'snuba-query-lock:'
query_cache_prefix = 'snuba-query-cache:'
config_hash = 'snuba-config'
queries_list = 'snuba-queries'

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
    bucket = '{}{}'.format(ratelimit_prefix, bucket)
    query_id = uuid.uuid4()
    now = time.time()

    pipe = rds.pipeline(transaction=False)
    pipe.zremrangebyscore(bucket, '-inf', '({:f}'.format(now - rate_history_s))  # cleanup
    pipe.zadd(bucket, now + max_query_duration_s, query_id)  # add query
    pipe.zcount(bucket, now - rate_lookback_s, now)  # get rate
    pipe.zcount(bucket, '({:f}'.format(now), '+inf')  # get concurrent
    try:
        _, _, rate, concurrent = pipe.execute()
    except Exception as ex:
        logger.error(ex)
        yield (True, 0, 0) # fail open if redis is having issues
        return

    per_second = rate / float(rate_lookback_s)
    allowed = (per_second_limit is None or per_second <= per_second_limit) and\
        (concurrent_limit is None or concurrent <= concurrent_limit)
    try:
        yield (allowed, per_second, concurrent)
    finally:
        try:
            if allowed:
                # return the query to its start time
                rds.zincrby(bucket, query_id, -float(max_query_duration_s))
            else:
                rds.zrem(bucket, query_id)  # not allowed / not counted
        except Exception as ex:
            logger.error(ex)
            pass


def get_concurrent(bucket):
    now = time.time()
    bucket = '{}{}'.format(ratelimit_prefix, bucket)
    return rds.zcount(bucket, '({:f}'.format(now), '+inf')


def get_rates(bucket, rollup=60):
    now = int(time.time())
    bucket = '{}{}'.format(ratelimit_prefix, bucket)
    pipe = rds.pipeline(transaction=False)
    for i in reversed(range(now - rollup, now - rate_history_s, -rollup)):
        pipe.zcount(bucket, i, '({:f}'.format(i + rollup))
    return [c / float(rollup) for c in pipe.execute()]

def _int(value):
    try:
        return int(value)
    except ValueError:
        return value

def set_config(key, value):
    if value is None:
        delete_config(key)
    else:
        try:
            rds.hset(config_hash, key, value)
        except Exception as ex:
            logger.error(ex)
            pass


def set_configs(values):
    for k, v in six.iteritems(values):
        set_config(k, v)


def get_config(key, default=None):
    try:
        result = rds.hget(config_hash, key)
        if result is not None:
            return _int(result)
    except Exception as ex:
        logger.error(ex)
        pass
    return default


def get_configs(key_defaults):
    configs = rds.hmget(config_hash, *[kd[0] for kd in key_defaults])
    return [key_defaults[i][1] if c is None else _int(c) for i, c in enumerate(configs)]


def get_all_configs():
    all_configs = rds.hgetall(config_hash)
    return {k: _int(v) for k, v in six.iteritems(all_configs)}

def delete_config(key):
    try:
        rds.hdel(config_hash, key)
    except Exception as ex:
        logger.error(ex)
        pass


def record_query(data):
    max_queries = 200
    data = json.dumps(data, for_json=True)
    try:
        rds.pipeline(transaction=False)\
            .lpush(queries_list, data)\
            .ltrim(queries_list, 0, max_queries - 1)\
            .execute()
    except Exception as ex:
        logger.error(ex)
        pass


def get_queries():
    try:
        queries = []
        for q in rds.lrange(queries_list, 0, -1):
            try:
                queries.append(json.loads(q))
            except BaseException:
                pass
    except Exception as ex:
        logger.error(ex)

    return queries

def get_result(query_id):
    key = '{}{}'.format(query_cache_prefix, query_id)
    result = rds.get(key)
    return result

def set_result(query_id, result):
    timeout = 1
    key = '{}{}'.format(query_cache_prefix, query_id)
    return rds.set(key, result, ex=timeout)

@contextmanager
def deduper(query_id):
    """
    A simple redis distributed lock on a query_id to prevent multiple
    concurrent queries running with the same id. Blocks subsequent
    queries until the first is finished.

    When used in conjunction with caching this means that the subsequent
    queries can then use the cached result from the first query.
    """

    unlock = '''
        if redis.call('get', KEYS[1]) == ARGV[1]
        then
            return redis.call('del', KEYS[1])
        else
            return 0
        end
    '''

    if query_id is None:
        yield False
    else:
        lock = '{}{}'.format(query_lock_prefix, query_id)
        nonce = uuid.uuid4()
        try:
            is_dupe = False
            while not rds.set(lock, nonce, nx=True, ex=max_query_duration_s):
                is_dupe = True
                time.sleep(0.01)
            yield is_dupe
        finally:
            rds.eval(unlock, 1, lock, nonce)
