from snuba.pipeline.settings_delegator import RateLimiterDelegate
from snuba.request.request_settings import HTTPRequestSettings
from snuba.state.rate_limit import RateLimitParameters


def test_delegate() -> None:
    settings = HTTPRequestSettings(
        referrer="test",
        turbo=False,
        consistent=False,
        debug=True,
        parent_api="parent",
        dry_run=False,
        legacy=False,
        team="team",
        feature="feature",
    )

    settings.add_rate_limit(
        RateLimitParameters(
            rate_limit_name="rate_name",
            bucket="project",
            per_second_limit=10.0,
            concurrent_limit=22,
        )
    )

    settings_delegate = RateLimiterDelegate("secondary", settings)
    settings_delegate.add_rate_limit(
        RateLimitParameters(
            rate_limit_name="second_rate_name",
            bucket="table",
            per_second_limit=11.0,
            concurrent_limit=23,
        )
    )

    assert settings_delegate.referrer == settings.referrer
    assert settings_delegate.get_rate_limit_params() == [
        RateLimitParameters(
            rate_limit_name="rate_name",
            bucket="secondary_project",
            per_second_limit=10.0,
            concurrent_limit=22,
        ),
        RateLimitParameters(
            rate_limit_name="second_rate_name",
            bucket="secondary_table",
            per_second_limit=11.0,
            concurrent_limit=23,
        ),
    ]
