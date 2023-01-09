from dataclasses import dataclass, field
from typing import Set

from snuba.admin.auth_roles import Role


@dataclass
class AdminUser:
    """
    Basic encapsulation of a user of the admin panel. In the future,
    should be extended to contain permissions among other things
    """

    email: str
    id: str
    roles: Set[Role] = field(default_factory=set)
