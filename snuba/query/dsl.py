from typing import Optional, Sequence

from snuba.query.expressions import FunctionCall, Literal

# Add here functions (only stateless stuff) used to make the AST less
# verbose to build.


def literals_tuple(alias: Optional[str], literals: Sequence[Literal]):
    return FunctionCall(alias, "tuple", tuple(literals))
