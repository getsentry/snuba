from __future__ import annotations

import threading
from collections.abc import Callable, Iterable
from typing import Any

import simplejson as json
import structlog

logger = structlog.get_logger().bind(module=__name__)

StartResponse = Callable[..., Any]
WSGIApp = Callable[[dict[str, Any], StartResponse], Iterable[bytes]]

_TIMEOUT_BODY = json.dumps(
    {"error": {"type": "timeout", "message": "Operation took too long"}}
).encode("utf-8")


class RequestTimeoutMiddleware:
    """
    WSGI middleware that caps the wall-clock duration of a single request.

    The wrapped app runs in a worker thread; if it doesn't finish within
    ``timeout_seconds`` we abandon it and return 504 Gateway Timeout to the
    client. The abandoned thread keeps running to completion (WSGI has no way
    to preempt it), but the underlying ClickHouse queries are themselves capped,
    so it unwinds on its own shortly after.
    """

    def __init__(self, wsgi_app: WSGIApp, timeout_seconds: int) -> None:
        self.wsgi_app = wsgi_app
        self.timeout_seconds = timeout_seconds

    def __call__(self, environ: dict[str, Any], start_response: StartResponse) -> Iterable[bytes]:
        captured: dict[str, Any] = {}
        error: dict[str, BaseException] = {}

        def run() -> None:
            def capture_start_response(
                status: str,
                headers: list[tuple[str, str]],
                exc_info: Any = None,
            ) -> Callable[[bytes], None]:
                captured["status"] = status
                captured["headers"] = headers
                return lambda _: None

            try:
                app_iter = self.wsgi_app(environ, capture_start_response)
                try:
                    captured["body"] = b"".join(app_iter)
                finally:
                    close = getattr(app_iter, "close", None)
                    if close is not None:
                        close()
            except BaseException as e:  # surfaced to the main thread below
                error["error"] = e

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join(self.timeout_seconds)

        if thread.is_alive():
            logger.warning(
                "admin request exceeded timeout",
                path=environ.get("PATH_INFO"),
                timeout_seconds=self.timeout_seconds,
            )
            start_response(
                "408 REQUEST TIMEOUT",
                [
                    ("Content-Type", "application/json"),
                    ("Content-Length", str(len(_TIMEOUT_BODY))),
                ],
            )
            return [_TIMEOUT_BODY]

        if "error" in error:
            raise error["error"]

        start_response(captured["status"], captured["headers"])
        return [captured["body"]]
