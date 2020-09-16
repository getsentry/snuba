import re

import _strptime  # NOQA fixes _strptime deferred import issue

QUALIFIED_COLUMN_REGEX = re.compile(
    r"^([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z0-9_\.\[\]]+)$"
)
