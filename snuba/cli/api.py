from typing import Optional

import os
import click

from snuba.environment import setup_logging


@click.command()
@click.option("--debug", is_flag=True)
@click.option("--log-level", help="Logging level to use.")
@click.option("--processes", default=1)
@click.option("--threads", default=1)
def api(*, debug: bool, log_level: Optional[str], processes: int, threads: int) -> None:
    from snuba import settings, state

    if debug:
        if processes > 1 or threads > 1:
            raise click.ClickException("processes/threads can only be 1 in debug")

        from snuba.web.views import application
        from werkzeug.serving import WSGIRequestHandler

        setup_logging(log_level)

        if settings.DEBUG or settings.TESTING:
            state.set_config("use_readthrough_query_cache", 0)

        WSGIRequestHandler.protocol_version = "HTTP/1.1"
        application.run(port=settings.PORT, threaded=True, debug=debug)
    else:
        import mywsgi

        if log_level:
            os.environ["LOG_LEVEL"] = log_level

        if settings.DEBUG or settings.TESTING:
            state.set_config("use_readthrough_query_cache", 0)

        mywsgi.run(
            "snuba.web.wsgi:application",
            f"0.0.0.0:{settings.PORT}",
            processes=processes,
            threads=threads,
        )
