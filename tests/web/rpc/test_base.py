from google.protobuf.timestamp_pb2 import Timestamp

from snuba.web.rpc import RPCEndpoint


class MyRPC(RPCEndpoint[Timestamp, Timestamp]):
    @classmethod
    def version(cls) -> str:
        return "v1"


def test_endpoint_name_resolution() -> None:
    assert RPCEndpoint.get_from_name("MyRPC", "v1") is MyRPC


def test_before_and_after_execute() -> None:
    before_called = False
    after_called = False

    class BeforeAndAfter(RPCEndpoint[Timestamp, Timestamp]):
        @classmethod
        def version(cls) -> str:
            return "v1"

        def _before_execute(self, in_msg: Timestamp):
            nonlocal before_called
            before_called = True

        def _execute(self, in_msg: Timestamp) -> Timestamp:
            return in_msg

        def _after_execute(self, in_msg: Timestamp, out_msg: Timestamp) -> Timestamp:
            nonlocal after_called
            after_called = True
            return out_msg

    BeforeAndAfter().execute(Timestamp())
    assert before_called
    assert after_called
