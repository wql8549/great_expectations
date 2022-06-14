from typing import List
from unittest import mock

from packaging import version

import great_expectations

# @mock.patch.object(great_expectations.core.usage_statistics.schema_versions, "SCHEMA_VERSIONS", ["5", "10"])
from great_expectations.core.usage_statistics.schema_version_checker import (
    _most_recent_schema_version,
)

# @mock.patch("great_expectations.core.usage_statistics.schema_versions.SCHEMA_VERSIONS", ["5.1.1", "10.0.3"])

# @mock.patch("great_expectations.core.usage_statistics.schema_version_checker._most_recent_schema_version")
# def test_is_schema_version_updated_successfully(schema_versions: str):
@mock.patch(
    "great_expectations.core.usage_statistics.schema_versions.LATEST_SCHEMA_VERSION",
    "10.0.3",
)
def test_is_schema_version_updated_successfully():

    d = great_expectations.core.usage_statistics.schema_versions.LATEST_SCHEMA_VERSION
    print(d)
    # schema_versions = ["5.1.1", "10.0.3"]
    # schema_versions.return_value = "5.1.2"

    # sv = version.parse(schema_versions)
    # print(sv)
    # print(_most_recent_schema_version())
    # a = schema_versions
    # print(a)
