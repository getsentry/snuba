from dataclasses import dataclass, field
from typing import Sequence

from snuba.admin.auth_roles import Role


@dataclass
class AdminUser:
    """
    Basic encapsulation of a user of the admin panel. In the future,
    should be extended to contain permissions among other things
    """

    email: str
    id: str
    roles: Sequence[Role] = field(default_factory=list)
