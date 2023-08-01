import click

from snuba.celery import celery_app


@click.command()
def worker() -> None:
    args = ["worker", "--loglevel=INFO"]
    celery_app.worker_main(argv=args)
