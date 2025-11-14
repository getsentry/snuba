import pytest

from snuba.clickhouse.errors import ClickhouseError

message1 = """
"DB::Exception: Limit for rows or bytes to read exceeded, max bytes: 11.92 GiB, current bytes: 12.77 GiB: While executing Remote. Stack trace:\n\n0. DB::Exception::Exception(DB::Exception::MessageMasked&&, int, bool) @ 0x000000000c61ff37 in /usr/bin/clickhouse\n1. DB::Exception::Exception<char const*&, ReadableSize, ReadableSize>(int, FormatStringHelperImpl<std::type_identity<char const*&>::type, std::type_identity<ReadableSize>::type
"""

message2 = """
"DB::Exception: Timeout exceeded: elapsed 25.218463468 seconds, maximum: 25. Stack trace:\n\n0. DB::Exception::Exception(DB::Exception::MessageMasked&&, int, bool) @ 0x000000000c61ff37 in /usr/bin/clickhouse\n1. DB::Exception::Exception<double, double>(int, FormatStringHelperImpl<std::type_identity<double>::type, std::type_identity<double>::type>, double&&, double&&) @ 0x0000000011309202 in /usr/bin/clickhouse\n2. DB::ExecutionSpeedLimits::checkTimeLimit(Stopwatch const&, DB::OverflowMode) const @ 0x0000000011309120 in /usr/bin/clickhouse\n3. DB::PipelineExecutor::finalizeExecution() @ 0x0000000013193a51 in /usr/bin/clickhouse\n4. DB::PipelineExecutor::execute(unsigned long, bool) @ 0x00000000131937f0 in /usr/bin/clickhouse\n5. void std::__function::__policy_invoker<void ()>::__call_impl<std::__function::__default_alloc_func<ThreadFromGlobalPoolImpl<true>::ThreadFromGlobalPoolImpl<DB
"""


@pytest.mark.parametrize(
    "message,expected_value",
    [
        pytest.param(
            message1,
            'Code: 1. "DB::Exception: Limit for rows or bytes to read exceeded, max bytes: 11.92 GiB, current bytes: 12.77 GiB: While executing Remote."',
            id="limit_exceeded",
        ),
        pytest.param(
            message2,
            'Code: 1. "DB::Exception: Timeout exceeded: elapsed 25.218463468 seconds, maximum: 25."',
            id="timeout_exceeded",
        ),
    ],
)
def test_message_formatter(message: str, expected_value: str) -> None:
    error = ClickhouseError(message, code=1)
    assert error.message == expected_value
