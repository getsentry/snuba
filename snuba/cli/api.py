import os
from typing import Optional, Union

import click

from snuba.environment import setup_logging
from snuba.utils import uwsgi


@click.command()
@click.option("--bind", help="Address to listen on.")
@click.option("--debug", is_flag=True)
@click.option("--log-level", help="Logging level to use.")
@click.option("--processes", default=1)
@click.option("--threads", default=1)
def api(
    *,
    bind: Optional[str],
    debug: bool,
    log_level: Optional[str],
    processes: int,
    threads: int,
) -> None:
    from snuba import settings

    port: Union[int, str]
    if bind:
        if ":" in bind:
            host, port = bind.split(":", 1)
            port = int(port)
        else:
            raise click.ClickException("bind can only be in the format <host>:<port>")
    else:
        host, port = settings.HOST, settings.PORT

    if debug:
        if processes > 1 or threads > 1:
            raise click.ClickException("processes/threads can only be 1 in debug")

        from werkzeug.serving import WSGIRequestHandler

        from snuba.web.views import application

        setup_logging(log_level)

        WSGIRequestHandler.protocol_version = "HTTP/1.1"
        application.run(host=host, port=port, threaded=True, debug=debug)
    else:
        if log_level:
            os.environ["LOG_LEVEL"] = log_level

        uwsgi.run(
            "snuba.web.wsgi:application",
            f"{host}:{port}",
            processes=processes,
            threads=threads,
        )
