"""Github handlers."""

from __future__ import annotations

from datetime import datetime

from github import Github


def curation_latest_tag(curation_repo: str = "opentargets/curation") -> str:
    """Get latest tag from curation repository.

    Arguments:
        curation_repo (str): Name of the curation repo.

    Returns:
        str: latest tag.
    """
    g = Github()
    repo = g.get_repo(curation_repo)

    tags: dict[datetime, str] = {}
    for tag in repo.get_tags():
        tag_name = tag.name
        commit = tag.commit
        commit_date = commit.commit.committer.date
        tags[commit_date] = tag_name
    last_commit_date = max(tags.keys())
    return tags[last_commit_date]
