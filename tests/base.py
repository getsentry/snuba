from typing import Any, Callable


class BaseApiTest:
    def setup_method(self, test_method: Callable[..., Any]) -> None:
        from snuba.web.views import application

        assert application.testing is True
        application.config["PROPAGATE_EXCEPTIONS"] = False
        self.app = application.test_client()
