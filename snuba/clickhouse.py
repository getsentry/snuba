from clickhouse_driver import Client

from snuba import settings


class Clickhouse(object):
    def __enter__(self):
        self.clickhouse = Client(
            host=settings.CLICKHOUSE_SERVER.split(':')[0],
            port=int(settings.CLICKHOUSE_SERVER.split(':')[1]),
            connect_timeout=1,
        )

        return self.clickhouse

    def __exit__(self, *args):
        self.clickhouse.disconnect()
