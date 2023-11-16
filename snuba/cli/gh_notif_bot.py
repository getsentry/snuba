import os
from typing import Optional

import click

from snuba.environment import setup_logging


@click.command()
@click.option("--debug", is_flag=True)
@click.option("--log-level", help="Logging level to use.")
def gh_notif_bot(
    *,
    debug: bool,
    log_level: Optional[str],
) -> None:
    host = os.environ.get("GH_BOT_HOST", "0.0.0.0")
    port = int(os.environ.get("GH_BOT_PORT", 3000))

    setup_logging(log_level)

    from werkzeug.serving import WSGIRequestHandler

    from snuba.bot.views import application

    WSGIRequestHandler.protocol_version = "HTTP/1.1"
    application.run(host=host, port=port, threaded=True, debug=debug)
