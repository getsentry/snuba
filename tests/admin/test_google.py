from googleapiclient.discovery import build
from googleapiclient.http import HttpMock

from snuba.admin.google import check_transitive_membership, get_group_id


def test_get_group_id() -> None:
    http = HttpMock("tests/admin/data/mock_responses/group_lookup_200.json")
    service = build("cloudidentity", "v1", http=http, developerKey="api_key")
    assert get_group_id(service, "group_email") == "groups/group_id"

    http = HttpMock("tests/admin/data/mock_responses/group_lookup_403.json")
    service = build("cloudidentity", "v1", http=http, developerKey="api_key")
    assert get_group_id(service=service, group_email="group_email") == ""


def test_check_transitive_membership() -> None:
    http = HttpMock(
        "tests/admin/data/mock_responses/check_transitive_membership_200.json"
    )
    service = build("cloudidentity", "v1", http=http, developerKey="api_key")
    assert check_transitive_membership(
        service=service, group_resource_name="groups/group_id", member="member"
    )
