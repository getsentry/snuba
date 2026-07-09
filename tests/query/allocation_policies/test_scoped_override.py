from snuba.query.allocation_policies.utils import resolve_scoped_override

# {organization_id (or "*"): {referrer (or "*"): value}}
SCOPED = {
    "123": {"api.foo": 500, "*": 1000},
    "*": {"api.foo": 2000},
}


def test_org_and_referrer_is_most_specific() -> None:
    assert resolve_scoped_override(SCOPED, 123, "api.foo", -1) == 500


def test_org_wildcard_referrer() -> None:
    # (123, "api.bar") has no exact entry, falls back to (123, "*")
    assert resolve_scoped_override(SCOPED, 123, "api.bar", -1) == 1000


def test_referrer_across_all_orgs() -> None:
    # org 999 has no entry, falls back to ("*", "api.foo")
    assert resolve_scoped_override(SCOPED, 999, "api.foo", -1) == 2000


def test_precedence_prefers_org_over_all_orgs() -> None:
    # both ("123","*")=1000 and ("*","api.bar") is absent; org-scoped wins
    assert resolve_scoped_override(SCOPED, 123, "api.bar", -1) == 1000
    # ("*","api.foo")=2000 exists but ("123","api.foo")=500 is more specific
    assert resolve_scoped_override(SCOPED, 123, "api.foo", -1) == 500


def test_falls_back_to_default() -> None:
    assert resolve_scoped_override(SCOPED, 999, "api.bar", -1) == -1
    assert resolve_scoped_override({}, 123, "api.foo", -1) == -1


def test_none_org_or_referrer_skips_those_lookups() -> None:
    # No org: only ("*", referrer) can match.
    assert resolve_scoped_override(SCOPED, None, "api.foo", -1) == 2000
    assert resolve_scoped_override(SCOPED, None, "api.bar", -1) == -1
    # No referrer: only (org, "*") can match.
    assert resolve_scoped_override(SCOPED, 123, None, -1) == 1000
    assert resolve_scoped_override(SCOPED, 999, None, -1) == -1


def test_org_id_is_stringified() -> None:
    # Integer org id resolves against the string JSON key.
    assert resolve_scoped_override({"123": {"*": 7}}, 123, "anything", -1) == 7
