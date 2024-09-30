from snuba.web.rpc import RPCEndpoint
from google.protobuf.timestamp_pb2 import Timestamp


class MyRPC(RPCEndpoint[Timestamp, Timestamp]):
    @classmethod
    def version(cls) -> str:
        return "v1"


def test_endpoint() -> None:
    assert RPCEndpoint.get_from_name("MyRPC", "v1") is MyRPC
