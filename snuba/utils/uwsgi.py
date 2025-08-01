from __future__ import annotations

import os
import sys
from typing import MutableMapping, NoReturn

PYUWSGI_PROG = """\
import os
import sys

orig = sys.getdlopenflags()
sys.setdlopenflags(orig | os.RTLD_GLOBAL)
try:
    import pyuwsgi
finally:
    sys.setdlopenflags(orig)

pyuwsgi.run()
"""


def _prepare_environ(
    options: dict[str, bool | int | str | None], env: MutableMapping[str, str]
) -> None:
    for k, v in options.items():
        if v is None:
            continue
        key = f"UWSGI_{k.upper().replace('-', '_')}"
        if isinstance(v, str):
            value = v
        elif v is True:
            value = "true"
        elif v is False:
            value = "false"
        elif isinstance(v, int):
            value = str(v)
        else:
            raise TypeError("Unknown option type: %r (%s)" % (k, type(v)))

        env.setdefault(key, value)


def run(module: str, bind: str, **kwargs: bool | int | str | None) -> NoReturn:
    protocol = os.environ.pop("UWSGI_PROTOCOL", "http")
    autoreload = os.environ.pop("UWSGI_AUTORELOAD", None)
    if autoreload is not None:
        kwargs["py_autoreload"] = int(autoreload)
    options: dict[str, bool | int | str | None] = {
        "auto_procname": True,
        "binary_path": sys.executable,
        "chmod_socket": 777,
        "die_on_term": True,
        "disable_write_exception": True,
        "enable_threads": True,
        "ignore_sigpipe": True,
        "ignore_write_errors": True,
        "lazy_apps": True,
        "log_format": '%(addr) - %(user) [%(ltime)] "%(method) %(uri) %(proto)" %(status) %(size) "%(referer)" "%(uagent)"',
        "log_x_forwarded_for": True,
        "master": True,
        "module": module,
        "need_app": True,
        "processes": 1,
        "protocol": protocol,
        "single_interpreter": True,
        "threads": 1,
        "thunder_lock": True,
        "vacuum": True,
        "virtualenv": sys.prefix,
        "wsgi_env_behavior": "holy",
        f"{protocol}_socket": bind,
        **kwargs,
    }

    _prepare_environ(options, os.environ)

    # TODO: https://github.com/lincolnloop/pyuwsgi-wheels/pull/17
    cmd = (sys.executable, "-c", PYUWSGI_PROG)
    os.execvp(cmd[0], cmd)
