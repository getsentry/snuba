from snuba.web import QueryResult
from snuba.clickhouse.query import Query


class PipelineStage[Tin, Tout]:

    def execute(self, in: TIN) -> Tout:
        pass

    def output(self) -> Tout:
        pass


class Translator(Query, Query):
    pass




class Format(Query, (str, Query)):
    pass

class Caching((str, Query), )


class QueryPipeline:

    stages = [
        Format,
        Caching,
        RateLimit,
        Execute
    ]


    ]
    def execute(self):
        Format >> Caching >> RateLimit >> Execute
# stuff
        pass


class SnqlToClickHousePipeline:

    stages = [
        SnqlToQuery,
        Optimezer,
        RateLimiter,
        Experiment

    ]
