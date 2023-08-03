from abc import ABC, abstractmethod
from typing import Any, List, MutableMapping, Optional, Sequence

from snuba.state.quota import ResourceQuota
from snuba.state.rate_limit import RateLimitParameters


class QuerySettings(ABC):
    """
    Settings that apply to how the query in the request should be run.
    The settings provided in this class do not directly affect the SQL statement that will be created
    (i.e. they do not directly appear in the SQL statement).
    They can indirectly affect the SQL statement that will be formed. For example, `turbo` affects
    the formation of the query for projects, but it doesn't appear in the SQL statement.
    """

    referrer: str

    @abstractmethod
    def get_turbo(self) -> bool:
        pass

    @abstractmethod
    def get_consistent(self) -> bool:
        pass

    @abstractmethod
    def get_debug(self) -> bool:
        pass

    @abstractmethod
    def get_dry_run(self) -> bool:
        pass

    @abstractmethod
    def get_legacy(self) -> bool:
        pass

    @abstractmethod
    def get_rate_limit_params(self) -> Sequence[RateLimitParameters]:
        pass

    @abstractmethod
    def add_rate_limit(self, rate_limit_param: RateLimitParameters) -> None:
        pass

    @abstractmethod
    def get_resource_quota(self) -> Optional[ResourceQuota]:
        pass

    @abstractmethod
    def set_resource_quota(self, quota: ResourceQuota) -> None:
        pass

    @abstractmethod
    def get_clickhouse_settings(self) -> MutableMapping[str, Any]:
        pass

    @abstractmethod
    def set_clickhouse_settings(self, settings: MutableMapping[str, Any]) -> None:
        pass

    @abstractmethod
    def get_asynchronous(self) -> bool:
        pass


# TODO: I don't like that there are two different classes for the same thing
# this could probably be replaces with a `source` attribute on the class
class HTTPQuerySettings(QuerySettings):
    """
    Settings that are applied to all Requests initiated via the HTTP api. Allows
    parameters to be customized, defaults to using global rate limits and allows
    additional rate limits to be added.
    """

    def __init__(
        self,
        turbo: bool = False,
        consistent: bool = False,
        debug: bool = False,
        dry_run: bool = False,
        # TODO: is this flag still relevant?
        legacy: bool = False,
        referrer: str = "unknown",
        asynchronous: bool = False,
    ) -> None:
        super().__init__()
        self.__turbo = turbo
        self.__consistent = consistent
        self.__debug = debug
        self.__dry_run = dry_run
        self.__legacy = legacy
        self.__rate_limit_params: List[RateLimitParameters] = []
        self.__resource_quota: Optional[ResourceQuota] = None
        self.__clickhouse_settings: MutableMapping[str, Any] = {}
        self.referrer = referrer
        self.__asynchronous = asynchronous

    def get_turbo(self) -> bool:
        return self.__turbo

    def get_consistent(self) -> bool:
        return self.__consistent

    def get_debug(self) -> bool:
        return self.__debug

    def get_dry_run(self) -> bool:
        return self.__dry_run

    def get_legacy(self) -> bool:
        return self.__legacy

    def get_rate_limit_params(self) -> Sequence[RateLimitParameters]:
        return self.__rate_limit_params

    def add_rate_limit(self, rate_limit_param: RateLimitParameters) -> None:
        self.__rate_limit_params.append(rate_limit_param)

    def get_resource_quota(self) -> Optional[ResourceQuota]:
        return self.__resource_quota

    def set_resource_quota(self, quota: ResourceQuota) -> None:
        self.__resource_quota = quota

    def get_clickhouse_settings(self) -> MutableMapping[str, Any]:
        return self.__clickhouse_settings

    def set_clickhouse_settings(self, settings: MutableMapping[str, Any]) -> None:
        self.__clickhouse_settings = settings

    def get_asynchronous(self) -> bool:
        return self.__asynchronous


class SubscriptionQuerySettings(QuerySettings):
    """
    Settings that are applied to Requests initiated via Subscriptions. Hard code most
    parameters and skips all rate limiting.
    """

    def __init__(
        self,
        consistent: bool = True,
        team: str = "workflow",
        feature: str = "subscription",
        app_id: str = "default",
        referrer: str = "subscription",
    ) -> None:
        self.__consistent = consistent
        self.__team = team
        self.__feature = feature
        self.__app_id = app_id
        self.referrer = referrer
        self.__clickhouse_settings: MutableMapping[str, Any] = {}

    def get_turbo(self) -> bool:
        return False

    def get_consistent(self) -> bool:
        return self.__consistent

    def get_debug(self) -> bool:
        return False

    def get_dry_run(self) -> bool:
        return False

    def get_legacy(self) -> bool:
        return False

    def get_team(self) -> str:
        return self.__team

    def get_app_id(self) -> str:
        return self.__app_id

    def get_feature(self) -> str:
        return self.__feature

    def get_rate_limit_params(self) -> Sequence[RateLimitParameters]:
        return []

    def add_rate_limit(self, rate_limit_param: RateLimitParameters) -> None:
        pass

    def get_resource_quota(self) -> Optional[ResourceQuota]:
        return None

    def set_resource_quota(self, quota: ResourceQuota) -> None:
        pass

    def get_clickhouse_settings(self) -> MutableMapping[str, Any]:
        return self.__clickhouse_settings

    def set_clickhouse_settings(self, settings: MutableMapping[str, Any]) -> None:
        self.__clickhouse_settings = settings

    def get_asynchronous(self) -> bool:
        return False
