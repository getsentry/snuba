from snuba import settings, util

"""
The generalizer attempts to rewrite some queries as more general queries
that can be cached and used to serve results for multiple similar queries.

For example, a single page might generate a set of queries like:

    SELECT count() FROM db WHERE tag[user] IS NOT NULL;
    SELECT count() FROM db WHERE tag[platform] IS NOT NULL;
    SELECT count() FROM db WHERE tag[logger] IS NOT NULL;

We can generalize the first one we see to be the more general query:

    SELECT tags_key, count() FROM db GROUP BY tags_key WHERE tags_value IS NOT NULL;

The result of this single query can be used to return results for all the more
specific queries to follow.
"""

def generalize(func):
    def wrapper(*args, **kwargs):
        """
            First experiment: replace a query for a simple count() of a
            single tag field with a count() GROUP BY tags_key
        """
        tag = None
        body = args[0]
        if (
                len(body['aggregations']) == 1 and
                body['aggregations'][0][:2] == ['count()', ''] and
                len(body['conditions']) == 1 and
                settings.NESTED_COL_EXPR.match(body['conditions'][0][0]) and
                body['conditions'][0][1:] == ['!=', ''] and
                body['selected_columns'] == [] and
                util.to_list(body['groupby']) == []
            ):

            agg = body['aggregations'][0][2]
            col, tag = settings.NESTED_COL_EXPR.match(body['conditions'][0][0]).group(1, 2)
            if col == 'tags':
                body['conditions'] = [['tags_value', '!=', '']]
                body['groupby'] = 'tags_key'
            # TODO Change any LIMIT to LIMIT N BY tags_key

        result, status = func(*args, **kwargs)
        if tag is not None:
            result['data'] = [{agg: d[agg]} for d in result['data'] if d['tags_key'] == tag]

        return result, status
    return wrapper


