class ToyJob:
    def __init__(self, dry_run: bool, storage_name: str) -> None:
        self.dry_run = dry_run
        self.storage_name = storage_name

    def __build_query(self) -> str:
        if self.dry_run:
            return "dry run query"
        else:
            return "not dry run query"

    def execute(self) -> str:
        return (
            "executing query "
            + self.__build_query()
            + "on storage "
            + self.storage_name
        )
