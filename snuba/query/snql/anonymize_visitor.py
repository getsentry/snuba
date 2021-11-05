from datetime import date, datetime

from snuba.query.expressions import Literal, StringifyVisitor


class AnonymizeAndStringifySnQLVisitor(StringifyVisitor):
    def visit_literal(self, exp: Literal) -> str:
        literal_str = None
        if isinstance(exp.value, str):
            literal_str = "$S"
        elif isinstance(exp.value, datetime):
            literal_str = "$DT"
        elif isinstance(exp.value, date):
            literal_str = "$D"
        elif isinstance(exp.value, bool):
            literal_str = "$B"
        elif isinstance(exp.value, int) or isinstance(exp.value, float):
            literal_str = "$N"
        else:
            literal_str = "None"
        res = f"{self._get_line_prefix()}{literal_str}{self._get_alias_str(exp)}"
        return res
