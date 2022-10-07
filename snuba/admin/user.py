from dataclasses import dataclass


@dataclass
class AdminUser:
    """
    Basic encapsulation of a user of the admin panel. In the future,
    should be extended to contain permissions among other things
    """

    email: str
    id: str
