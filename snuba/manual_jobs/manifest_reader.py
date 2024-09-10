import os
from typing import Any, Sequence

import simplejson


class _ManifestReader:
    @staticmethod
    def read(filename: str) -> Sequence[Any]:
        local_root = os.path.dirname(__file__)
        with open(os.path.join(local_root, filename)) as stream:
            contents = simplejson.loads(stream.read())
            assert isinstance(contents, Sequence)
            return contents


def read_jobs_manifest() -> Sequence[Any]:
    return _ManifestReader.read("run_manifest.json")
