from urllib.parse import urlencode

from googleapiclient.discovery import Resource


def check_transitive_membership(service: Resource, parent: str, member: str) -> bool:
    try:
        query_params = urlencode({"query": "member_key_id == '{}'".format(member)})
        request = (
            service.groups().memberships().checkTransitiveMembership(parent=parent)
        )
        request.uri += "&" + query_params
        response = request.execute()
        return bool(response["hasMembership"])
    except Exception as e:
        print(e)

    return False


def get_group_id(service: Resource, group_email: str) -> str:
    try:
        query_params = f"groupKey.id={group_email}"
        request = service.groups().lookup()
        request.uri += "&" + query_params
        response = request.execute()
        return str(response["name"])
    except Exception as e:
        print(e)

    return ""
