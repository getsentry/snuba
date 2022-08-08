from dataclasses import dataclass


@dataclass
class EntityKey:
    value: str

    def __hash__(self):
        return hash(self.value)


class _EntityKeys:
    # TODO: Add comments and explain the history/decision
    def __getattr__(self, attr: str) -> EntityKey:
        return EntityKey(attr.lower())


EntityKeys = _EntityKeys()
