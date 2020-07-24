class InvalidJsonRequestException(Exception):
    """
    Common parent class for exceptions signaling the json payload
    of the request was not valid.
    """

    pass


class JsonDecodeException(InvalidJsonRequestException):
    pass


class JsonSchemaValidationException(InvalidJsonRequestException):
    pass
