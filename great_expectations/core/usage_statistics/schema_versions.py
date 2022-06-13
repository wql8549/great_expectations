from typing import List

# When to update SCHEMA_VERSION?
# Schema versions follow semver: MAJOR.MINOR.PATCH
# PATCH - if GE is updated and schemas.py is changed
# MINOR - if GE is updated to a new minor or major version (regardless of schemas.py change)
# MAJOR - break in case of emergency

SCHEMA_VERSIONS: List[str] = ["1.0.0", "1.0.1"]
