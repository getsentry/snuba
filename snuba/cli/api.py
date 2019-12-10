import click


@click.command()
@click.option("--debug", is_flag=True)
def api(*, debug: bool) -> None:
    from snuba import settings
    from snuba.views import application
    from werkzeug.serving import WSGIRequestHandler

    WSGIRequestHandler.protocol_version = "HTTP/1.1"
    application.run(port=settings.PORT, threaded=True, debug=debug)
