import os
from typing import Optional

import click

from snuba.environment import setup_logging


@click.command()
@click.option("--debug", is_flag=True)
@click.option("--log-level", help="Logging level to use.")
@click.option("--processes", default=1)
@click.option("--threads", default=1)
def admin(
    *, debug: bool, log_level: Optional[str], processes: int, threads: int,
) -> None:
    from snuba import settings

    host, port = settings.ADMIN_HOST, settings.ADMIN_PORT

    if debug:
        if processes > 1 or threads > 1:
            raise click.ClickException("processes/threads can only be 1 in debug")

        from werkzeug.serving import WSGIRequestHandler

        from snuba.web.views import application

        setup_logging(log_level)

        WSGIRequestHandler.protocol_version = "HTTP/1.1"
        application.run(host=host, port=port, threaded=True, debug=debug)
    else:
        import mywsgi

        if log_level:
            os.environ["LOG_LEVEL"] = log_level

        mywsgi.run(
            "snuba.admin.wsgi:application",
            f"{host}:{port}",
            processes=processes,
            threads=threads,
        )
