from typing import List, Optional, Sequence

from snuba.utils.describer import Description, DescriptionVisitor, Property


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

    def visit_property(self, property: Property) -> None:
        self.__content.append(f"{property.name} {property.value}")


def test_describer() -> None:
    desc = Description(
        header="my_header",
        content=[
            "string1",
            Property("key", "value"),
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
