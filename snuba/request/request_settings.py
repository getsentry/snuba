from abc import ABC, abstractmethod
from typing import List, Optional, Sequence

from snuba.state.quota import ResourceQuota
from snuba.state.rate_limit import RateLimitParameters


class RequestSettings:
    """Sentinel class which exists for legacy purposes"""
    pass


class HTTPRequestSettings(RequestSettings):
    pass


class SubscriptionRequestSettings(RequestSettings):
    pass
