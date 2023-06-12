from googleapiclient.discovery import build
from googleapiclient.http import HttpMock

from snuba.admin.google import CloudIdentityAPI


def test_get_group_id() -> None:
    http = HttpMock("tests/admin/data/mock_responses/group_lookup_200.json")
    service = build("cloudidentity", "v1", http=http, developerKey="api_key")
    api_client = CloudIdentityAPI(service=service)
    assert api_client._get_group_id("group_email") == "groups/group_id"

    http = HttpMock("tests/admin/data/mock_responses/group_lookup_403.json")
    service = build("cloudidentity", "v1", http=http, developerKey="api_key")
    api_client = CloudIdentityAPI(service=service)
    assert not api_client._get_group_id(group_email="group_email")


def test_check_transitive_membership() -> None:
    http = HttpMock(
        "tests/admin/data/mock_responses/check_transitive_membership_200.json"
    )
    service = build("cloudidentity", "v1", http=http, developerKey="api_key")
    api_client = CloudIdentityAPI(service=service)
    assert api_client._check_transitive_membership(
        group_resource_name="groups/group_id", member="member"
    )


def test_check_group_membership() -> None:
    http = HttpMock("tests/admin/data/mock_responses/group_lookup_403.json")
    service = build("cloudidentity", "v1", http=http, developerKey="api_key")
    api_client = CloudIdentityAPI(service=service)
    assert not api_client.check_group_membership(
        group_email="group_email", member="member"
    )
