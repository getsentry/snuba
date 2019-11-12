import click

from snuba import settings


@click.command()
@click.option('--debug', is_flag=True)
@click.option('--port', type=int, default=settings.PORT)
def api(*, debug: bool, port: int):
    from snuba.views import application
    from werkzeug.serving import WSGIRequestHandler

    WSGIRequestHandler.protocol_version = "HTTP/1.1"
    application.run(port=port, threaded=True, debug=debug)
