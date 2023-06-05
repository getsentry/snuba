"""

API modification proposal for adapting to multiple policies per storage.


A key assumption here is that each storage only uses 1 instance of a certain policy.
Eg. You cannot have two BytesScannedAllocationPolicy's on a single storage.

If we cannot assume this, we need to implement a new way to uniquely identify an instance
of an AllocationPolicy - maybe with some sort of `name` property on the class.

"""


# returns name + policy name per storage
# /allocation_policies
[
    {
        "storage_name": "discover",
        "allocation_policy": "BytesScannedWindowAllocationPolicy",
    },
    ...,
]

"""
The current flow for this is to display the name of the storage and policy at the top and then
render a new component with the name of the storage to actually get all of the configs.

This endpoint could be modified to just return a list of storages that have allocation policies
and call another endpoint to return configs grouped by policy name which can help render further
components.
"""

# new version: new route + new response
# /storages_with_allocation_policies
[
    "discover",
    ...,
]

# ------------------------------------------------------------------------------------------------------------------------------------------------------

# returns current configs for storage
# /allocation_policy_configs/<storage_key>
[
    {
        "name": "throttled_thread_number",
        "type": "int",
        "default": 1,
        "description": "Number of threads any throttled query gets assigned.",
        "value": 1,
        "params": {},
    },
    ...,
]

# returns optional config definitions for storage
# /allocation_policy_optional_config_definitions/<storage_key>

[
    {
        "name": "org_limit_bytes_scanned_override",
        "type": "int",
        "default": -1,
        "description": "Number of bytes a specific org can scan in a 10 minute window.",
        "params": [{"name": "org_id", "type": "int"}],
    },
    ...,
]

"""
These two API routes can be consolidated into one, they are called at the same time and modify
stuff in the same way.

This could be modified to return configs and optional config defs grouped by policy name.
Upon selecting a storage previously, new components would render per policy returned by this api.
"""

# new version: same api but new response
# /allocation_policy_configs/<storage_key>
[
    {
        "policy_name": "BytesScannedWindowPolicy",
        "configs": [
            {
                "name": "throttled_thread_number",
                "type": "int",
                "default": 1,
                "description": "Number of threads any throttled query gets assigned.",
                "value": 1,
                "params": {},
            },
            ...,
        ],
        "optional_config_definitions": [
            {
                "name": "org_limit_bytes_scanned_override",
                "type": "int",
                "default": -1,
                "description": "Number of bytes a specific org can scan in a 10 minute window.",
                "params": [{"name": "org_id", "type": "int"}],
            },
            ...,
        ],
    },
    ...,
]

# ------------------------------------------------------------------------------------------------------------------------------------------------------

# Adds or Deletes a config via POST or DELETE respectively
# /allocation_policy_config

# POST payload
{
    "storage": "errors_ro",
    "key": "org_limit_bytes_scanned_override",
    "value": "111",
    "params": {"org_id": "1"},
}

"""
This would basically just need to be modified to add the policy name in the payload.
"""

# new payload for POST
{
    "storage": "errors_ro",
    "policy": "BytesScannedAllocationPolicy",
    "key": "org_limit_bytes_scanned_override",
    "value": "111",
    "params": {"org_id": "1"},
}

# ------------------

# DELETE payload

# very similar to POST payload, but doesn't include the value
{
    "storage": "errors_ro",
    "key": "org_limit_bytes_scanned_override",
    "params": {"org_id": "1"},
}

"""
Similarly, this would be modified to just add policy name.
"""

{
    "storage": "errors_ro",
    "policy": "BytesScannedAllocationPolicy",
    "key": "org_limit_bytes_scanned_override",
    "params": {"org_id": "1"},
}
