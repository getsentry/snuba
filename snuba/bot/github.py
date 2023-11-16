from __future__ import annotations

from functools import partial
from typing import Any, Callable, Generator

import structlog
from ghapi.all import GhApi  # type: ignore

MARK_READ = False
CHECK_REVIEWERS = True

logger = structlog.get_logger().bind(module=__name__)


def initialize_github(github_token: str) -> tuple[GhApi, str]:
    api = GhApi(token=github_token)
    user_login = api.users.get_authenticated()
    return api, user_login["login"]


def fetch_notifications(api: GhApi, github_login: str) -> list[dict[str, Any]]:
    to_review = []
    puller = partial(api.activity.list_notifications_for_authenticated_user)
    valid_reasons = set(["review_requested", "commented", "mentioned"])
    for notif in paginate(puller):
        if notif["reason"] not in valid_reasons:
            mark_read(notif, f"{notif['reason']} not valid reason")
            continue

        if notif["subject"]["type"] != "PullRequest":
            mark_read(notif, "not a pull request")
            continue

        pull_url = notif["subject"]["url"]
        owner, repo, _, pull_number = pull_url.split("/")[-4:]

        pull_details = api.pulls.get(owner=owner, repo=repo, pull_number=pull_number)

        if pull_details["state"] != "open":
            mark_read(notif, "PR not open")
            continue

        if pull_details["user"]["login"] == github_login:
            mark_read(notif, "self PR")
            continue

        # - The PR can't have been reviewed by other people already
        # unless the user is one of the reviewers
        # Will need to filter for bots
        # Need to also check if reviewers are owners-snuba
        # also needs to handle app-backend, python-typing
        # basically: was the PR already reviwed by someone who has the authority to approve the PR (handles bots)

        """
        pull_details["requested_teams"]
        - This has a "slug" and an "id"

        pull all reviews and review comments
        For each user, see if they belong to one of the requested teams
        If they are, assume they can approve the PR
        """
        review_puller = partial(
            api.pulls.list_reviews,
            owner=owner,
            repo=repo,
            pull_number=pull_number,
        )
        self_reviewed = False
        reviews = 0
        for review in paginate(review_puller):
            reviews += 1
            if review["user"]["login"] == github_login:
                self_reviewed = True

        if reviews > 0 and not self_reviewed:
            mark_read(notif, "PR already reviewed")
            continue

        if pull_details["comments"] > 0:
            comment_puller = partial(
                api.pulls.list_review_comments,
                owner=owner,
                repo=repo,
                pull_number=pull_number,
            )
            self_commented = False
            for comment in paginate(comment_puller):
                if comment["user"]["login"] == github_login:
                    self_commented = True

            if not self_commented:
                mark_read(notif, "PR already commented")
                continue

        # The notification doesn't have a html_url, so we need to add in the one from the PR
        notif["subject"]["html_url"] = pull_details["html_url"]
        to_review.append(notif)

    return to_review


def paginate(fetcher: Callable[..., Any]) -> Generator[Any, None, None]:
    page = 1
    while True:
        items = fetcher(page=page)
        if not items:
            break
        yield from items
        page += 1


def mark_read(notif: dict[str, Any], reason: str = "") -> None:
    logger.info(f"Rejected: {reason} | {notif['subject']['title']}")
    return
    # if MARK_READ:
    #     g.api.activity.mark_thread_as_read(thread_id=notif["id"])
    # else:
