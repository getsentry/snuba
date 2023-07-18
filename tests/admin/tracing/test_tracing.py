from snuba.admin.clickhouse.tracing import format_trace_output


def test_formatted_trace() -> None:
    with open("tests/admin/tracing/example_raw_trace.txt", "r") as file:
        contents = file.read()
        formatted_trace_output = format_trace_output(contents)
        query_node_name = ""
        storage_node_names = []
        for node in formatted_trace_output:
            if formatted_trace_output[node]["node_type"] == "query":
                query_node_name = node
            else:
                storage_node_names.append(node)

        # Check formatted traces from query node
        assert query_node_name == "snuba-gen-metrics-query-0-1-4"
        assert formatted_trace_output[query_node_name]["threads_used"] == 2
        assert (
            formatted_trace_output[query_node_name]["number_of_storage_nodes_accessed"]
            == 4
        )
        assert set(
            formatted_trace_output[query_node_name]["storage_nodes_accessed"]
        ) == set(storage_node_names)
        assert (
            len(formatted_trace_output[query_node_name]["aggregation_performance"]) > 0
        )
        assert (
            "executeQuery: Read"
            in formatted_trace_output[query_node_name]["read_performance"][0]
        )
        assert (
            "MemoryTracker: Peak"
            in formatted_trace_output[query_node_name]["memory_performance"][0]
        )

        # Check formatted traces from storage node
        for storage_node in storage_node_names:
            assert formatted_trace_output[storage_node]["threads_used"] == 2
            assert len(formatted_trace_output[storage_node]["key_conditions"]) > 0
            assert len(formatted_trace_output[storage_node]["skip_indexes"]) > 0
            assert len(formatted_trace_output[storage_node]["filtering_algorithm"]) > 0
            assert (
                "parts by partition key"
                in formatted_trace_output[storage_node]["selected_parts_and_marks"][0]
            )
            assert (
                "Aggregation method"
                in formatted_trace_output[storage_node]["aggregation_method"][0]
            )
            assert (
                "AggregatingTransform"
                in formatted_trace_output[storage_node]["aggregation_performance"][0]
            )

            assert (
                "executeQuery: Read"
                in formatted_trace_output[storage_node]["read_performance"][0]
            )
            assert (
                "MemoryTracker: Peak"
                in formatted_trace_output[storage_node]["memory_performance"][0]
            )
