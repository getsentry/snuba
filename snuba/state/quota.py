from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResourceQuota:
    """
    Tracks the quota a client can use when running a query.

    As of now we only represent that in threads.
    """

    max_threads: int
