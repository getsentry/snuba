import click


@click.command()
@click.option('--debug', is_flag=True)
@click.option('--processes', default=1)
def api(debug, processes):
    from snuba import settings

    if debug:
        if processes > 1:
            raise click.ClickException("processes can only be 1 in debug")

        from snuba.api import application
        from werkzeug.serving import WSGIRequestHandler

        WSGIRequestHandler.protocol_version = "HTTP/1.1"
        application.run(port=settings.PORT, threaded=True, debug=debug)
    else:
        import mywsgi
        mywsgi.run(
            "snuba.api:application",
            "0.0.0.0:%d" % settings.PORT,
            processes=processes,
        )
