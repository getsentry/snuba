from typing import Any

import click

from snuba.manual_jobs import Job


class ToyJob(Job):
    def __init__(self, dry_run: bool, **kwargs: Any):
        super().__init__(dry_run, **kwargs)

    def _build_query(self) -> str:
        if self.dry_run:
            return "dry run query"
        else:
            return "not dry run query"

    def execute(self) -> None:
        click.echo("executing query `" + self._build_query() + "`")
