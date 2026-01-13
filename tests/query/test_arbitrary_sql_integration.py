"""
Integration tests for ArbitrarySQL in full query contexts
"""

from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.query.expressions import ArbitrarySQL, Column, FunctionCall, Literal


def test_arbitrary_sql_in_select() -> None:
    """Test ArbitrarySQL used in SELECT clause"""
    formatter = ClickhouseExpressionFormatter()

    expressions = [
        FunctionCall(None, "count", ()),
        ArbitrarySQL("custom", "arrayReduce('sum', array)"),
        Column(None, "t", "col"),
    ]

    formatted = [exp.accept(formatter) for exp in expressions]

    assert formatted[0] == "count()"
    assert formatted[1] == "(arrayReduce('sum', array) AS custom)"
    assert formatted[2] == "t.col"


def test_arbitrary_sql_query_optimization_scenario() -> None:
    """
    Test realistic query optimization scenario where ArbitrarySQL
    would be used to inject optimized ClickHouse-specific SQL
    """
    formatter = ClickhouseExpressionFormatter()

    # Original: FunctionCall for aggregation
    # Optimized: Replace with ArbitrarySQL using ClickHouse-specific syntax

    # Simulating: arrayReduce('quantilesTiming(0.5, 0.95)', measurements)
    optimized = ArbitrarySQL("quantiles", "arrayReduce('quantilesTiming(0.5, 0.95)', measurements)")

    result = optimized.accept(formatter)
    expected = "(arrayReduce('quantilesTiming(0.5, 0.95)', measurements) AS quantiles)"
    assert result == expected


def test_arbitrary_sql_preserves_special_syntax() -> None:
    """Test that ArbitrarySQL preserves ClickHouse-specific syntax"""
    formatter = ClickhouseExpressionFormatter()

    # Test various ClickHouse-specific features
    test_cases = [
        # Array operators
        "arr[1]",
        # Map operators
        "map['key']",
        # Tuple access
        "tuple.1",
        # Lambda in arrayMap
        "arrayMap(x -> x * 2, arr)",
        # JSON operators
        "json->>'field'",
        # Settings in function
        "sum(col) SETTINGS optimize_aggregation_in_order=1",
    ]

    for sql in test_cases:
        exp = ArbitrarySQL(None, sql)
        result = exp.accept(formatter)
        assert result == sql, f"Failed for: {sql}"


def test_arbitrary_sql_nested_in_expressions() -> None:
    """Test ArbitrarySQL nested in complex expressions"""
    formatter = ClickhouseExpressionFormatter()

    # Nested ArbitrarySQL: if(ArbitrarySQL, column, ArbitrarySQL)
    condition = ArbitrarySQL(None, "has(array, value)")
    col = Column(None, "t", "col1")
    default = ArbitrarySQL(None, "defaultValue()")

    func = FunctionCall(None, "if", (condition, col, default))
    result = func.accept(formatter)

    expected = "if(has(array, value), t.col1, defaultValue())"
    assert result == expected


def test_arbitrary_sql_with_multiline() -> None:
    """Test that ArbitrarySQL preserves multiline SQL"""
    formatter = ClickhouseExpressionFormatter()

    sql_multiline = """arrayFilter(
    x -> x > 0,
    arr
)"""
    exp = ArbitrarySQL(None, sql_multiline)
    result = exp.accept(formatter)

    assert result == sql_multiline


def test_arbitrary_sql_combination() -> None:
    """Test combining multiple ArbitrarySQL expressions in one function"""
    formatter = ClickhouseExpressionFormatter()

    # Create a complex expression combining ArbitrarySQL and regular expressions
    arb1 = ArbitrarySQL(None, "custom_func1()")
    arb2 = ArbitrarySQL("alias2", "custom_func2()")
    col = Column(None, "t", "col")
    literal = Literal(None, 100)

    outer = FunctionCall("result", "greatest", (arb1, arb2, col, literal))

    result = outer.accept(formatter)
    expected = "(greatest(custom_func1(), (custom_func2() AS alias2), t.col, 100) AS result)"
    assert result == expected


def test_arbitrary_sql_empty_string() -> None:
    """Test that empty ArbitrarySQL is handled correctly"""
    formatter = ClickhouseExpressionFormatter()

    exp = ArbitrarySQL(None, "")
    result = exp.accept(formatter)

    assert result == ""


def test_arbitrary_sql_with_quotes_and_escapes() -> None:
    """Test ArbitrarySQL with quotes and escape sequences"""
    formatter = ClickhouseExpressionFormatter()

    # SQL with single quotes, double quotes, and backslashes
    sql = r"replaceRegexpAll(col, '\\s+', ' ')"
    exp = ArbitrarySQL(None, sql)
    result = exp.accept(formatter)

    # Should be passed through unchanged (no escaping)
    assert result == sql
