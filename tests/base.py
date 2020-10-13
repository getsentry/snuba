class BaseApiTest:
    def setup_method(self, test_method):
        from snuba.web.views import application

        assert application.testing is True
        application.config["PROPAGATE_EXCEPTIONS"] = False
        self.app = application.test_client()
