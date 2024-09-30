from snuba.web.rpc import RPCEndpoint


class MyRPC(RPCEndpoint[int, str]):
    @classmethod
    def version(cls):
        return "v1"


def test_endpoint() -> None:
    assert RPCEndpoint.get_from_name("MyRPC", "v1") is MyRPC
