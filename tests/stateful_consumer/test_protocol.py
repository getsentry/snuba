import jsonschema
import pytest
import json

from snuba.stateful_consumer.control_protocol import (
    parse_control_message,
    SnapshotInit,
    SnapshotAbort,
    SnapshotLoaded,
    TransactionData
)


class TestControlProtocol:
    def test_init(self):
        message = (
            '{ '
            '"event": "snapshot-init", '
            '"product": "snuba", '
            '"snapshot-id": "b63d0b22-53f3-48c1-810c-de593f9c54df" '
            '}'
        )
        parsed_message = parse_control_message(json.loads(message))
        assert isinstance(parsed_message, SnapshotInit)
        assert parsed_message.id == "b63d0b22-53f3-48c1-810c-de593f9c54df"
        assert parsed_message.product == "snuba"

    def test_abort(self):
        message = (
            '{ '
            '"event": "snapshot-abort", '
            '"snapshot-id": "b63d0b22-53f3-48c1-810c-de593f9c54df" '
            '}'
        )
        parsed_message = parse_control_message(json.loads(message))
        assert isinstance(parsed_message, SnapshotAbort)
        assert parsed_message.id == "b63d0b22-53f3-48c1-810c-de593f9c54df"

    def test_loaded(self):
        message = (
            '{'
            '"event": "snapshot-loaded",'
            '"snapshot-id": "b63d0b22-53f3-48c1-810c-de593f9c54df",'
            '"datasets": {'
            '   "groupedmessages": {'
            '       "temp_table": "temp_groupedmessage"'
            '   }'
            '},'
            '"transaction-info": {'
            '   "xmin": 123123,'
            '   "xmax": 123132,'
            '   "xip-list": [ 123124 ]'
            '}'
            '}'
        )
        parsed_message = parse_control_message(json.loads(message))
        assert isinstance(parsed_message, SnapshotLoaded)
        assert parsed_message.id == "b63d0b22-53f3-48c1-810c-de593f9c54df"
        assert parsed_message.datasets == {
            "groupedmessages": {
                "temp_table": "temp_groupedmessage",
            }
        }
        assert parsed_message.transaction_info == TransactionData(
            xmin=123123,
            xmax=123132,
            xip_list=[123124],
        )

    def test_bad_data(self):
        message = '{"event":"garbage"}'
        with pytest.raises(jsonschema.ValidationError):
            parse_control_message(json.loads(message))
