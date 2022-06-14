import subprocess
from typing import List

from packaging import version

import great_expectations
from great_expectations.core.usage_statistics.schema_versions import SCHEMA_VERSIONS


def _most_recent_schema_version() -> version.Version:
    """Retrieve the most recent schema version.

    Returns:
        Version object of the latest schema version.
    """
    return sorted([version.parse(v) for v in SCHEMA_VERSIONS])[-1]


def _previous_schema_version() -> version.Version:
    """Retrieve the most recent schema version.

    Returns:
        Version object of the latest schema version.
    """
    return sorted([version.parse(v) for v in SCHEMA_VERSIONS])[-2]


def _num_schema_versions() -> int:
    """Number of schema versions supported in events."""
    return len(SCHEMA_VERSIONS)


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


def _is_schema_version_changed() -> bool:
    PREVIOUS_NUMBER_OF_VERSIONS = 1

    if _num_schema_versions() > PREVIOUS_NUMBER_OF_VERSIONS:
        assert (
            _num_schema_versions() == PREVIOUS_NUMBER_OF_VERSIONS + 1
        ), "Only one schema version should be added at a time."
        return True

    return False


def _is_schema_patch_version_changed() -> bool:
    most_recent_schema_version: version.Version = _most_recent_schema_version()
    previous_schema_version: version.Version = _previous_schema_version()

    return most_recent_schema_version.micro > previous_schema_version.micro


def _is_schema_minor_version_changed() -> bool:
    most_recent_schema_version: version.Version = _most_recent_schema_version()
    previous_schema_version: version.Version = _previous_schema_version()

    return most_recent_schema_version.minor > previous_schema_version.minor


def _equal_or_one_greater(item: int, check_value: int) -> bool:
    """Check if item is equal or one greater than check_value

    Args:
        item:
        check_value:

    Returns:

    """
    return (item == check_value) or (item == (check_value + 1))


def _did_ge_update_major_or_minor_version() -> bool:

    BASE_MAJOR_VERSION = 0
    BASE_MINOR_VERSION = 15
    current_base_version: version.Version = version.parse(
        version.parse(great_expectations.__version__).base_version
    )
    assert _equal_or_one_greater(current_base_version.minor, BASE_MINOR_VERSION)
    assert _equal_or_one_greater(current_base_version.major, BASE_MAJOR_VERSION)
    return (current_base_version.minor > BASE_MINOR_VERSION) or (
        current_base_version.major > BASE_MAJOR_VERSION
    )


def _should_schema_patch_version_be_updated() -> bool:
    return _is_schemas_py_changed()


def _should_schema_minor_version_be_updated() -> bool:
    return _did_ge_update_major_or_minor_version()


def is_schema_version_updated_successfully() -> bool:

    if _should_schema_minor_version_be_updated() and _is_schema_minor_version_changed():
        return True
    if _should_schema_patch_version_be_updated() and _is_schema_patch_version_changed():
        return True

    return False


if __name__ == "__main__":
    pass
    # import great_expectations as ge

    # print(version.parse(ge.__version__).base_version)
    # print(_get_previous_version())
    # print(_get_changed_files())
    # print(_is_schema_version_changed())
