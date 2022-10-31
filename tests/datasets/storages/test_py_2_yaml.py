from abc import ABCMeta
from copy import deepcopy
from typing import Any, Dict, Tuple, Type

from snuba import settings


class InitRecorder(ABCMeta):
    """Records the arguments passed to the __init__ function of classes
    in an init_kwargs member. Only useful if said objects need to be converted
    to declarative configuration

    Handles positional arguments by inspecting the signature and turning them into kwargs
    for ease of use
    """

    def __new__(cls, name: str, bases: Tuple[Type[Any]], dct: Dict[str, Any]) -> Any:
        res = super().__new__(cls, name, bases, dct)
        # this is not useful in production in any way, hence this is a noop unless explicitly enabled
        if settings.ENABLE_INIT_RECORDER:
            from inspect import signature

            orig_init = getattr(res, "__init__")

            def __init__(self, *args, **kwargs):
                self.init_kwargs = deepcopy(kwargs)
                if args:
                    init_param_names = [p for p in signature(orig_init).parameters]
                    i = 0
                    while i < len(args):
                        self.init_kwargs[init_param_names[i + 1]] = args[i]
                        i += 1
                return orig_init(self, *args, **kwargs)

            setattr(res, "__init__", __init__)
        return res


def test_record_init_args():
    class A(metaclass=InitRecorder):
        def __init__(self, a, b):
            self.a = a
            self.b = b

    instance = A(a="a", b="b")
    assert instance.init_kwargs == {"a": "a", "b": "b"}

    instance2 = A("a", "b")
    assert instance2.init_kwargs == {"a": "a", "b": "b"}

    instance3 = A("a", b="b")
    assert instance3.init_kwargs == {"a": "a", "b": "b"}

    class NoInit(metaclass=InitRecorder):
        pass

    no_init_instance = NoInit()
    assert no_init_instance.init_kwargs == {}
