from snuba.utils.schemas import ColumnSet


class EntityColumnSet(ColumnSet):
    """
    Entity data model supports wildcard columns as well as the other types
    """

    def __repr__(self) -> str:
        return f"EntityColumnSet({repr(self.columns)})"
