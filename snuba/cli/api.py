from typing import Optional

import click

from snuba.environment import setup_logging


@click.command()
@click.option("--debug", is_flag=True)
@click.option("--log-level", help="Logging level to use.")
def api(*, debug: bool, log_level: Optional[str] = None) -> None:
    from snuba import settings
    from snuba.web.views import application
    from werkzeug.serving import WSGIRequestHandler

    setup_logging(log_level)

    WSGIRequestHandler.protocol_version = "HTTP/1.1"
    application.run(port=settings.PORT, threaded=True, debug=debug)
