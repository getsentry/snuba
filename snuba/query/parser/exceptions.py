class ParsingException(Exception):
    pass


class CyclicAliasException(ParsingException):
    pass
