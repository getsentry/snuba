import click

from snuba.workers import celery_app


@click.command()
def worker() -> None:
    args = ["worker", "--loglevel=INFO"]
    celery_app.worker_main(argv=args)
