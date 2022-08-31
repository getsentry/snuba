import os
import socket
from typing import Optional, Union

import click

from snuba.attribution.log import flush_attribution_producer
from snuba.environment import setup_logging


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
        if host.strip() == "0.0.0.0" or host.strip() == "127.0.0.1":
            _check_local_ports(port)

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

        with flush_attribution_producer():
            mywsgi.run(
                "snuba.web.wsgi:application",
                f"{host}:{port}",
                processes=processes,
                threads=threads,
            )


def _check_local_ports(port: int) -> None:
    # check that ports on both localhosts are free as it is almost always a mistake to
    # have both in use
    def _try_host(host: str) -> None:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind((host, port))
            sock.close()
        except OSError:
            raise click.ClickException(f"port {port} unavailable on host {host}")

    _try_host("127.0.0.1")
    _try_host("0.0.0.0")
