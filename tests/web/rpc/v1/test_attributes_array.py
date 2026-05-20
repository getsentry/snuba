from snuba.web.rpc.common.common import pop_attributes_array_paths


def test_pop_attributes_array_paths_ignores_non_list_gen_ai_paths() -> None:
    row = {
        "gen_ai.system_instructions": '"be concise"',
        "gen_ai.response.object": '"{\\"answer\\": \\"Paris\\"}"',
        "gen_ai.tool.call.arguments": '"{\\"location\\": \\"Paris\\"}"',
        "gen_ai.tool.input": '"{\\"location\\": \\"Paris\\"}"',
    }

    assert list(pop_attributes_array_paths(row)) == []
    assert row == {
        "gen_ai.system_instructions": '"be concise"',
        "gen_ai.response.object": '"{\\"answer\\": \\"Paris\\"}"',
        "gen_ai.tool.call.arguments": '"{\\"location\\": \\"Paris\\"}"',
        "gen_ai.tool.input": '"{\\"location\\": \\"Paris\\"}"',
    }


def test_pop_attributes_array_paths_returns_stringified_non_list_values() -> None:
    row = {"gen_ai.input.messages": '{"location": "Paris"}'}

    assert list(pop_attributes_array_paths(row)) == [
        ("gen_ai.input.messages", '{"location": "Paris"}')
    ]
    assert row == {}
