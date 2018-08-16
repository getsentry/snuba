from snuba import settings, state, util
import six

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
        First experiment, a non-grouped query with aggregations
        and a condition on a tags[] field is changed to the same
        query with GROUP BY tags_key, to effectively get the same
        data for all tags in one pass.
        """
        tag = None
        body = args[0]
        tag_conditions = [
            (c, settings.NESTED_COL_EXPR.match(c[0]).group(1, 2))
            for c in body['conditions']
            if c and isinstance(c[0], six.string_types) and
            settings.NESTED_COL_EXPR.match(c[0])
        ]
        aggregations = [agg[2] for agg in body['aggregations'] if agg[2] and agg[0] == 'count()']
        if (
                state.get_config('generalize_query', 0) and
                # no selected columns or exisiting groups
                body['selected_columns'] == [] and
                util.to_list(body['groupby']) == [] and
                # at least 1 aggretation and a tags[] type condition
                aggregations and tag_conditions and
                all(col == 'tags' for (cond, (col, tag)) in tag_conditions) and
                # all tags[] conditions refer to the same tag
                len(set(tag for (cond, (col, tag)) in tag_conditions)) == 1
            ):

            tag = tag_conditions[0][1][1]
            body['groupby'] = 'tags_key'
            for (cond, _) in tag_conditions:
                cond[0] = 'tags_key'
            if 'limit' in body:
                body['limitby'] = [body.pop('limit'), 'tags_key']

        result, status = func(*args, **kwargs)
        if tag is not None and 'data' in result:
            result['data'] = [
                {agg: d[agg] for agg in aggregations}
                for d in result['data'] if d['tags_key'] == tag
            ]

        return result, status
    return wrapper


