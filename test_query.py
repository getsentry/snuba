FunctionCall(
    None,
    "and",
    (
        FunctionCall(
            None,
            "notEquals",
            (
                FunctionCall(
                    None,
                    "ifNull",
                    (
                        FunctionCall(
                            "_snuba_tags[duration_group]",
                            "arrayElement",
                            (
                                Column(None, None, "tags.value"),
                                FunctionCall(
                                    None,
                                    "indexOf",
                                    (
                                        Column(None, None, "tags.key",),
                                        Literal(None, "duration_group",),
                                    ),
                                ),
                            ),
                        ),
                        Literal(None, ""),
                    ),
                ),
                Literal(None, ""),
            ),
        ),
        FunctionCall(
            None,
            "equals",
            (
                FunctionCall(
                    None,
                    "ifNull",
                    (
                        FunctionCall(
                            "_snuba_tags[duration_group]",
                            "arrayElement",
                            (
                                Column(None, None, "tags.value"),
                                FunctionCall(
                                    None,
                                    "indexOf",
                                    (
                                        Column(None, None, "tags.key",),
                                        Literal(None, "duration_group",),
                                    ),
                                ),
                            ),
                        ),
                        Literal(None, ""),
                    ),
                ),
                Literal(None, "<10s"),
            ),
        ),
    ),
)
