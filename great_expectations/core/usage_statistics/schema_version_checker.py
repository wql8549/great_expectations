import subprocess
from typing import List

from packaging import version

from great_expectations.core.usage_statistics.schema_versions import SCHEMA_VERSIONS


def _most_recent_schema_version() -> version.Version:
    """Retrieve the most recent schema version.

    Returns:
        Version object of the latest schema version.
    """
    return sorted([version.parse(v) for v in SCHEMA_VERSIONS])[-1]


def _get_changed_files(branch: str = "origin/develop") -> List[str]:
    """Perform a `git diff` against a given branch.

    Args:
        branch (str): The branch to diff against (generally `origin/develop`)

    Returns:
        A list of changed files.
    """
    git_diff: subprocess.CompletedProcess = subprocess.run(
        ["git", "diff", branch, "--name-only"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    return [f for f in git_diff.stdout.split()]


def _get_previous_version(branch: str = "origin/develop") -> List[str]:
    """Perform a `git diff` against a given branch.

    Args:
        branch (str): The branch to diff against (generally `origin/develop`)

    Returns:
        A list of changed files.
    """
    git_diff: subprocess.CompletedProcess = subprocess.run(
        ["git", "diff", branch, "schema_version"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    return [f for f in git_diff.stdout.split()]


def _is_schemas_py_changed(branch: str = "origin/develop") -> bool:
    """Is schemas.py changed?

    Args:
        branch (str): The branch to diff against (generally `origin/develop`)

    Returns:
        boolean
    """
    return "great_expectations/core/usage_statistics/schemas.py" in _get_changed_files(
        branch
    )


def _is_schema_patch_version_changed() -> bool:
    pass


if __name__ == "__main__":
    print(_get_previous_version())
