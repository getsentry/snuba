from typing import List, Optional, Sequence, Tuple

from snuba.utils.describer import Description, DescriptionVisitor


class TestDescriber(DescriptionVisitor):
    def __init__(self) -> None:
        self.__content: List[Optional[str]] = []

    def get_content(self) -> Sequence[Optional[str]]:
        return self.__content

    def visit_header(self, header: Optional[str]) -> None:
        self.__content.append(header)

    def visit_description(self, desc: Description) -> None:
        desc.accept(self)

    def visit_string(self, string: str) -> None:
        self.__content.append(string)

    def visit_tuple(self, tuple: Tuple[str, str]) -> None:
        self.__content.append(f"{tuple[0]} {tuple[1]}")


def test_describer() -> None:
    desc = Description(
        header="my_header",
        content=[
            "string1",
            ("key", "value"),
            Description(header="my_second_header", content=["string2"]),
        ],
    )

    describer = TestDescriber()
    desc.accept(describer)
    assert describer.get_content() == [
        "my_header",
        "string1",
        "key value",
        "my_second_header",
        "string2",
    ]
