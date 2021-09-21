from typing import Any, List, Mapping, Optional, Type, Union

from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, FunctionCall, Literal


class DatasetSubscription:
    def __init__(self, subscription_type: str, data_dict: Mapping[str, Any]) -> None:
        self.subscription_type = subscription_type

    def get_dataset_subscription_conditions(
        self, offset: Optional[int] = None
    ) -> List[Any]:
        return []

    def to_dict(self) -> Mapping[str, Any]:
        return {}


class SessionsSubscription(DatasetSubscription):
    organization: int

    def __init__(self, subscription_type: str, data_dict: Mapping[str, Any]):
        super().__init__(subscription_type, data_dict)
        try:
            self.organization = data_dict["organization"]
        except KeyError:
            raise InvalidQueryException(
                "organization param is required for any query over sessions dataset"
            )

    def get_dataset_subscription_conditions(
        self, offset: Optional[int] = None
    ) -> List[Any]:
        """
        Returns a list of extra conditions that are dataset specific and required for the
        subscription
        """
        conditions_to_add = []
        if self.subscription_type == "snql":
            conditions_to_add += [
                binary_condition(
                    ConditionFunctions.EQ,
                    Column("_snuba_org_id", None, "org_id"),
                    Literal(None, self.organization),
                ),
            ]
        return conditions_to_add

    def to_dict(self) -> Mapping[str, Any]:
        return {"organization": self.organization}


class EventsAndTransactionsSubscription(DatasetSubscription):
    def get_dataset_subscription_conditions(
        self, offset: Optional[int] = None
    ) -> List[Any]:
        """
        Returns a list of extra conditions that are dataset specific and required for the
        subscription
        """
        if offset is None:
            return []

        conditions_to_add: List[Any] = []
        if self.subscription_type == "snql":
            conditions_to_add.append(
                binary_condition(
                    ConditionFunctions.LTE,
                    FunctionCall(
                        None,
                        "ifNull",
                        (Column(None, None, "offset"), Literal(None, 0)),
                    ),
                    Literal(None, offset),
                )
            )
        elif self.subscription_type == "legacy":
            conditions_to_add = [[["ifnull", ["offset", 0]], "<=", offset]]
        return conditions_to_add


def get_dataset_subscription_class(
    data_dict: Mapping[str, Any]
) -> Union[Type[SessionsSubscription], Type[EventsAndTransactionsSubscription]]:
    """
    Function that returns the correct DatasetSubscription class based on the provided dictionary
    of data
    """
    # At the moment checking if organization is in the data_dict suffices to identify that
    #  SessionsSubscription class should be returned. However, If we were to extend the datasets
    #  that subscriptions support, we might need a better approach of checking.
    return (
        SessionsSubscription
        if "organization" in data_dict
        else EventsAndTransactionsSubscription
    )
