FunctionCall(
    None,
    "and",
    (
        FunctionCall(
            None,
            "greaterOrEquals",
            (
                Column("_snuba_timestamp", None, "timestamp"),
                Literal(None, datetime.datetime(2021, 7, 12, 19, 45, 1)),
            ),
        ),
        FunctionCall(
            None,
            "and",
            (
                FunctionCall(
                    None,
                    "less",
                    (
                        Column("_snuba_timestamp", None, "timestamp"),
                        Literal(None, datetime.datetime(2021, 8, 11, 19, 45, 1)),
                    ),
                ),
                FunctionCall(
                    None,
                    "and",
                    (
                        FunctionCall(
                            None,
                            "in",
                            (
                                Column("_snuba_project_id", None, "project_id"),
                                FunctionCall(
                                    None, "tuple", ({exp.parameters[0].accept(self)},),
                                ),
                            ),
                        ),
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
                                                        Column(
                                                            None, None, "tags.value"
                                                        ),
                                                        FunctionCall(
                                                            None,
                                                            "indexOf",
                                                            (
                                                                Column(
                                                                    None,
                                                                    None,
                                                                    "tags.key",
                                                                ),
                                                                Literal(
                                                                    None,
                                                                    "duration_group",
                                                                ),
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
                                                        Column(
                                                            None, None, "tags.value"
                                                        ),
                                                        FunctionCall(
                                                            None,
                                                            "indexOf",
                                                            (
                                                                Column(
                                                                    None,
                                                                    None,
                                                                    "tags.key",
                                                                ),
                                                                Literal(
                                                                    None,
                                                                    "duration_group",
                                                                ),
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
                        ),
                    ),
                ),
            ),
        ),
    ),
)
