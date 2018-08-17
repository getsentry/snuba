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
        TAG_RE = settings.NESTED_COL_EXPR
        generalized = False
        body = args[0]
        tag_cond = [
            c for c in body['conditions']
            if c and isinstance(c[0], six.string_types) and TAG_RE.match(c[0])
        ]
        aggs = body['aggregations']
        tag_aggs = [
            a for a in aggs
            if a and isinstance(a[1], six.string_types) and TAG_RE.match(a[1])
        ]
        tags = list(set([c[0] for c in tag_cond] + [a[1] for a in tag_aggs]))
        if tags:
            tag, tagcol, tagval = TAG_RE.match(tags[0]).group(0, 1, 2)

        if (
                state.get_config('generalize_query', 0) and
                # no selected columns
                body['selected_columns'] == [] and
                # at least 1 tags[] based condition
                tag_cond and
                # all aggregations have aliases
                all(alias for (_, _, alias) in aggs) and
                # only a single unique tag is used in any tags[] conditions or aggregations
                len(tags) == 1 and tagcol == 'tags' and tagval in settings.PROMOTED_TAGS[tagcol]
            ):

            generalized = True
            # replace all tags[] columns in conditions and aggregations with tags_value
            for cond in tag_cond:
                cond[0] = 'tags_value'
            for agg in tag_aggs:
                agg[1] = 'tags_value'


            fwd, rev = {tag: 'tags_value'}, {'tags_value': tag}
            groupby = util.to_list(body['groupby'])
            groupby = [fwd.get(gb, gb) for gb in groupby]

            output_columns = [alias for (_, _, alias) in aggs] + groupby
            # add `tags_key` to the grouping so that we get a set of results for each tag
            groupby.append('tags_key')
            body['groupby'] = groupby

            # Limit the tags to just promoted ones so that we don't blow up everything
            body['conditions'].append(['tags_key', 'IN', settings.PROMOTED_TAGS['tags']])

            if 'limit' in body:
                body['limitby'] = [body.pop('limit'), 'tags_key']


        result, status = func(*args, **kwargs)

        if generalized:
            if 'data' in result:
                result['data'] = [
                    {rev.get(col, col): d[col] for col in output_columns}
                    for d in result['data'] if d.get('tags_key') == tagval
                ]
            if 'meta' in result:
                result['meta'] = [m for m in result['meta'] if m['name'] in output_columns]

        return result, status
    return wrapper


