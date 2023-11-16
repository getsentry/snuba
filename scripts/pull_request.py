from __future__ import annotations

from functools import partial
from typing import Any, Callable, Generator

from ghapi.all import GhApi  # type: ignore

MARK_READ = False
api = GhApi()

user_login = api.users.get_authenticated()
self_login = user_login["login"]


def paginate(fetcher: Callable[..., Any]) -> Generator[Any, None, None]:
    page = 1
    while True:
        items = fetcher(page=page)
        if not items:
            break
        yield from items
        page += 1


def mark_read(notif: dict[str, Any], reason: str = "") -> None:
    if MARK_READ:
        api.activity.mark_thread_as_read(thread_id=notif["id"])
    else:
        print(
            "Would mark as read because", reason, notif["id"], notif["subject"]["title"]
        )


to_review = []
puller = partial(api.activity.list_notifications_for_authenticated_user)
for notif in paginate(puller):
    print(f"Checking {notif['subject']['title']}")
    # - Has to be review_requested,
    if notif["reason"] != "review_requested":
        mark_read(notif, "not review requested")
        continue

    # Has to be a pull request
    if notif["subject"]["type"] != "PullRequest":
        mark_read(notif, "not a pull request")
        continue

    pull_url = notif["subject"]["url"]
    owner, repo, _, pull_number = pull_url.split("/")[-4:]

    pull_details = api.pulls.get(owner=owner, repo=repo, pull_number=pull_number)

    # - the PR it references has to be open
    if pull_details["state"] != "open":
        mark_read(notif, "PR not open")
        continue

    # - The PR can't be a self PR
    if pull_details["user"]["login"] == self_login:
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

    # If it's already approved, skip it?
    if pull_details["review_comments"] > 0:
        review_puller = partial(
            api.pulls.list_reviews, owner=owner, repo=repo, pull_number=pull_number
        )
        self_reviewed = False
        for review in paginate(review_puller):
            if review["user"]["login"] == self_login:
                self_reviewed = True

        if not self_reviewed:
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
            if comment["user"]["login"] == self_login:
                self_commented = True

        if not self_commented:
            mark_read(notif, "PR already commented")
            continue

    to_review.append(notif)

print("Have", len(to_review), "PRs to review")
for review in to_review:
    print(review["subject"]["title"])
    # pprint.pprint(review)
    # if pull_details["review_comments"] == 0:
    #     pass  # probably add

    # pprint.pprint(notif)

    # print(f"{title} - {reason}")

# puller = partial(api.pulls.list, state="open")

# for pull in paginate(puller):
#     if pull["user"]["login"] != "evanh":
#         to_review.append((pull["number"], pull["user"]["login"], pull["title"]))

# print(f"{len(to_review)} Pull requests to review:")
# for number, user, title in to_review:
#     print(f"#{number} - {user} - {title}")
