from collections import deque
from datetime import datetime, timedelta
from hashlib import md5
import jsonschema
import logging
import re
import simplejson as json
import six
import _strptime  # fixes _strptime deferred import issue

from . import settings, schemas
from .clickhouse import escape_col, CLICKHOUSE_DATETIME_FORMAT
from .util import force_bytes, escape_string


logger = logging.getLogger('snuba.processor')


HASH_RE = re.compile(r'^[0-9a-f]{32}$', re.IGNORECASE)
MAX_UINT32 = 2 ** 32 - 1
EXCLUDE_GROUPS = object()
NEEDS_FINAL = object()


class EventTooOld(Exception):
    pass


class InvalidMessage(Exception):
    pass


class Processor(object):
    """
    The Processor is responsible for converting an incoming message body from the
    event stream into a row or statement to be inserted or executed agsinst clickhouseinserted or executed agsinst clickhouse.
    """

    # Message types returned by validate_message. INSERTs are
    # processed using process_insert(), and REPLACEs are forwarded
    # to a replacements topic to be consumed by the replacements
    # consumer and processed with process_replacement()
    INSERT = 0
    REPLACE = 1

    def __init__(self, SCHEMA):
        self.SCHEMA = SCHEMA

        # Regular consumer reads from here
        self.MESSAGE_TOPIC = None
        self.MESSAGE_CONSUMER_GROUP = None
        # Regular consumer writes replacements to, and replacements consumer
        # reads replacements from here.
        self.REPLACEMENTS_TOPIC = None
        self.REPLACEMENTS_CONSUMER_GROUP = None
        # Regular consumer writes committed offsets here, to trigger
        # post-processing tasks.
        self.COMMIT_LOG_TOPIC = None


    @property
    def key_function(self):
        """
        Return a function that can be invoked with a given message in order to produce a
        partitioning key for kafka for that message.
        """
        return lambda x: six.text_type(hash(x)).encode('utf-8')

    def validate_message(self, message):
        """
        Validate a raw mesage from kafka, with 1 of 3 results.
          - A tuple of (action_type, data) is returned to be handled by the proper
            handler/processor for that action type.
          - None is returned, indicating we should silently ignore this message.
          - An exeption is raised, inditcating something is wrong and we should stop.
        """
        raise NotImplementedError

    def process_insert(self, message):
        """
        Return a row object (conforming to the schema) to be inserted
        into clickhouse
        """
        raise NotImplementedError

    def process_replacement(self, message):
        """
        Return a tuple of:
        (count_query_template, insert_query_template, query_args, query_time_flags)
        that describes a set of queries that should be run against clickhouse
        to replace rows in the dataset with new ones
        """
        raise NotImplementedError


    # Processing utility functions
    @staticmethod
    def _as_dict_safe(value):
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        rv = {}
        for item in value:
            if item is not None:
                rv[item[0]] = item[1]
        return rv


    @staticmethod
    def _collapse_uint32(n):
        if (n is None) or (n < 0) or (n > MAX_UINT32):
            return None
        return n


    @staticmethod
    def _boolify(s):
        if s is None:
            return None

        if isinstance(s, bool):
            return s

        s = Processor._unicodify(s)

        if s in ('yes', 'true', '1'):
            return True
        elif s in ('false', 'no', '0'):
            return False

        return None


    @staticmethod
    def _floatify(s):
        if not s:
            return None

        if isinstance(s, float):
            return s

        try:
            s = float(s)
        except (ValueError, TypeError):
            return None
        else:
            return s

        return None


    @staticmethod
    def _unicodify(s):
        if s is None:
            return None

        if isinstance(s, dict) or isinstance(s, list):
            return json.dumps(s)

        return six.text_type(s)


    @staticmethod
    def _hashify(h):
        if HASH_RE.match(h):
            return h
        return md5(force_bytes(h)).hexdigest()


    @staticmethod
    def _ensure_valid_date(dt):
        if dt is None:
            return None
        seconds = (dt - datetime(1970, 1, 1)).total_seconds()
        if Processor._collapse_uint32(seconds) is None:
            return None
        return dt



