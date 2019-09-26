from dataclasses import dataclass

@dataclass(frozen=True)
class RequestSettings:
    turbo: bool
    consistent: bool
    debug: bool