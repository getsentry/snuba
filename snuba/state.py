from confluent_kafka import Producer
from contextlib import contextmanager
import logging
import random
import re
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
kfk = None

ratelimit_prefix = 'snuba-ratelimit:'
query_lock_prefix = 'snuba-query-lock:'
query_cache_prefix = 'snuba-query-cache:'
config_hash = 'snuba-config'
queries_list = 'snuba-queries'

##### Rate Limiting and Deduplication

# Window for concurrent query counting
max_query_duration_s = 60
# Window for determining query rate
rate_lookback_s = 60
# Amount of time we keep rate history
rate_history_s = 3600

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


##### Runtime Configuration

class memoize():
    """
    Simple expiring memoizer for functions with no args.
    """
    def __init__(self, timeout=1):
        self.timeout = timeout
        self.saved = None
        self.at = 0

    def __call__(self, func):
        def wrapper():
            now = time.time()
            if now > self.at + self.timeout or self.saved is None:
                self.saved, self.at = func(), now
            return self.saved
        return wrapper

def _int(value):
    try:
        return int(value)
    except ValueError:
        return value

ABTEST_RE = re.compile('(?:(\d+)(?:\:(\d+))?\/?)')
def abtest(value):
    """
    Recognizes a value that consists of a '/'-separated sequence of
    value:weight tuples, weight is optional. Returns a weighted random
    value from the set of values..
    eg.
    1000/2000 => returns 1000 or 2000 with equal weight
    1000:1/2000:1 => returns 1000 or 2000 with equal weight
    1000:2/2000:1 => returns 1000 twice as often as 2000
    """
    if isinstance(value, six.string_types) and ABTEST_RE.match(value):
        values = ABTEST_RE.findall(value)
        total_weight = sum(int(weight or 1) for (_, weight) in values)
        r = random.randint(1, total_weight)
        i = 0
        for (v, weight) in values:
            i += int(weight or 1)
            if i >= r:
                return _int(v)
    else:
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
    return abtest(get_all_configs().get(key, default))


def get_configs(key_defaults):
    all_confs = get_all_configs()
    return [abtest(all_confs.get(k, d)) for k, d in key_defaults]


@memoize(settings.CONFIG_MEMOIZE_TIMEOUT)
def get_all_configs():
    try:
        all_configs = rds.hgetall(config_hash)
        return {k: _int(v) for k, v in six.iteritems(all_configs) if v is not None}
    except Exception as ex:
        logger.error(ex)
        return {}

def delete_config(key):
    try:
        rds.hdel(config_hash, key)
    except Exception as ex:
        logger.error(ex)
        pass

##### Query Recording

def record_query(data):
    global kfk
    max_redis_queries = 200
    data = json.dumps(data, for_json=True)
    try:
        rds.pipeline(transaction=False)\
            .lpush(queries_list, data)\
            .ltrim(queries_list, 0, max_redis_queries - 1)\
            .execute()

        if settings.RECORD_QUERIES:
            if kfk is None:
                kfk = Producer({
                    'bootstrap.servers': ','.join(settings.DEFAULT_BROKERS)
                })

            kfk.produce(
                settings.QUERIES_TOPIC,
                data.encode('utf-8'),
            )
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
    return result and json.loads(result)

def set_result(query_id, result):
    timeout = get_config('cache_expiry_sec', 1)
    key = '{}{}'.format(query_cache_prefix, query_id)
    return rds.set(key, json.dumps(result), ex=timeout)
