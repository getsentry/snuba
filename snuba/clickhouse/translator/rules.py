from dataclasses import dataclass
from typing import Optional

from snuba.clickhouse.query import Expression
from snuba.datasets.plans.translator.mapping_rules import (
    SimpleExpressionMapper,
    StructuredExpressionMapper,
)
from snuba.query.dsl import array_element
from snuba.query.expressions import (
    Column,
    ExpressionVisitor,
    FunctionCall,
    SubscriptableReference,
)


@dataclass(frozen=True)
class ColumnMapper(SimpleExpressionMapper[Column, Expression]):
    from_col_name: str
    from_table_name: Optional[str]
    to_col_name: str
    to_table_name: Optional[str]

    def attemptMap(self, expression: Column) -> Optional[Expression]:
        if (
            expression.column_name == self.from_col_name
            and expression.table_name == self.from_table_name
        ):
            return Column(
                alias=expression.alias,
                table_name=self.to_table_name,
                column_name=self.to_col_name,
            )
        else:
            return None


@dataclass(frozen=True)
class TagMapper(StructuredExpressionMapper[SubscriptableReference, Expression]):
    tag_column_name: str
    tag_column_table: str

    def attemptMap(
        self,
        expression: SubscriptableReference,
        children_translator: ExpressionVisitor[Expression],
    ) -> Optional[Expression]:
        if (
            expression.column.column_name != self.tag_column_name
            or expression.column.table_name == self.tag_column_table
        ):
            return None

        return array_element(
            expression.alias,
            Column(None, f"{self.tag_column_name}.value", None),
            FunctionCall(
                None,
                "indexOf",
                (
                    Column(None, f"{self.tag_column_name}.key", None),
                    expression.key.accept(children_translator),
                ),
            ),
        )
