from sentry_sdk import init, capture_exception
init('http://cd8ff258cdbe7ec401de57cf915273ad@localhost:8000/1')
capture_exception(Exception("This is an example of an error message."))
