from snuba.datasets.factory import get_dataset
from snuba import state
from snuba.util import (
    column_expr,
    conditions_expr,
    tuplify,
)


def test_simple_column_expr():
    dataset = get_dataset("groups")
    state.set_config('use_escape_alias', 1)

    body = {
        'granularity': 86400
    }
    assert column_expr(dataset, "events.event_id", body.copy()) \
        == "(events.event_id AS `events.event_id`)"

    assert column_expr(dataset, "groups.id", body.copy()) \
        == "(groups.id AS `groups.id`)"

    assert column_expr(dataset, "events.event_id", body.copy(), "MyVerboseAlias") \
        == "(events.event_id AS MyVerboseAlias)"

    # Single tag expression
    assert column_expr(dataset, 'events.tags[foo]', body.copy()) ==\
        "(events.tags.value[indexOf(events.tags.key, \'foo\')] AS `events.tags[foo]`)"

    # Promoted tag expression / no translation
    assert column_expr(dataset, 'events.tags[server_name]', body.copy()) ==\
        "(events.server_name AS `events.tags[server_name]`)"

    # All tag keys expression
    assert column_expr(dataset, 'events.tags_key', body.copy()) == (
        '(arrayJoin(events.tags.key) AS `events.tags_key`)'
    )

    # If we are going to use both tags_key and tags_value, expand both
    tag_group_body = {
        'groupby': ['events.tags_key', 'events.tags_value']
    }
    assert column_expr(dataset, 'events.tags_key', tag_group_body) == (
        '(((arrayJoin(arrayMap((x,y) -> [x,y], events.tags.key, events.tags.value)) '
        'AS all_tags))[1] AS `events.tags_key`)'
    )

    assert column_expr(dataset, 'events.time', body.copy()) ==\
        "(toDate(events.timestamp) AS `events.time`)"

    assert column_expr(dataset, 'events.col', body.copy(), aggregate='sum') ==\
        "(sum(events.col) AS `events.col`)"

    assert column_expr(dataset, 'events.col', body.copy(), alias='summation', aggregate='sum') ==\
        "(sum(events.col) AS summation)"

    assert column_expr(dataset, '', body.copy(), alias='aggregate', aggregate='count()') ==\
        "(count() AS aggregate)"

    # Columns that need escaping
    assert column_expr(dataset, 'events.sentry:release', body.copy()) == '`events.sentry:release`'

    # A 'column' that is actually a string literal
    assert column_expr(dataset, '\'hello world\'', body.copy()) == '\'hello world\''

    # Complex expressions (function calls) involving both string and column arguments
    assert column_expr(dataset, tuplify(['concat', ['a', '\':\'', 'b']]), body.copy()) == 'concat(a, \':\', b)'

    group_id_body = body.copy()
    assert column_expr(dataset, 'events.issue', group_id_body) == '(nullIf(events.group_id, 0) AS `events.issue`)'

    # turn uniq() into ifNull(uniq(), 0) so it doesn't return null where a number was expected.
    assert column_expr(dataset, 'events.tags[environment]', body.copy(), alias='unique_envs', aggregate='uniq') == "(ifNull(uniq(events.environment), 0) AS unique_envs)"


def test_alias_in_alias():
    state.set_config('use_escape_alias', 1)
    dataset = get_dataset("groups")
    body = {
        'groupby': ['events.tags_key', 'events.tags_value']
    }
    assert column_expr(dataset, 'events.tags_key', body) == (
        '(((arrayJoin(arrayMap((x,y) -> [x,y], events.tags.key, events.tags.value)) '
        'AS all_tags))[1] AS `events.tags_key`)'
    )

    # If we want to use `tags_key` again, make sure we use the
    # already-created alias verbatim
    assert column_expr(dataset, 'events.tags_key', body) == '`events.tags_key`'
    # If we also want to use `tags_value`, make sure that we use
    # the `all_tags` alias instead of re-expanding the tags arrayJoin
    assert column_expr(dataset, 'events.tags_value', body) == '((all_tags)[2] AS `events.tags_value`)'


def test_conditions_expr():
    dataset = get_dataset("groups")
    state.set_config('use_escape_alias', 1)
    conditions = [['events.a', '=', 1]]
    assert conditions_expr(dataset, conditions, {}) == '(events.a AS `events.a`) = 1'

    conditions = [[['events.a', '=', 1], ['groups.b', '=', 2]], [['events.c', '=', 3], ['groups.d', '=', 4]]]
    assert conditions_expr(dataset, conditions, {}) \
        == ('((events.a AS `events.a`) = 1 OR (groups.b AS `groups.b`) = 2)'
        ' AND ((events.c AS `events.c`) = 3 OR (groups.d AS `groups.d`) = 4)'
        )

    # Test column expansion
    conditions = [[['events.tags[foo]', '=', 1], ['groups.b', '=', 2]]]
    expanded = column_expr(dataset, 'events.tags[foo]', {})
    assert conditions_expr(dataset, conditions, {}) \
        == '({} = 1 OR (groups.b AS `groups.b`) = 2)'.format(expanded)

    # Test using alias if column has already been expanded in SELECT clause
    reuse_body = {}
    conditions = [[['events.tags[foo]', '=', 1], ['groups.b', '=', 2]]]
    column_expr(dataset, 'events.tags[foo]', reuse_body)  # Expand it once so the next time is aliased
    assert conditions_expr(dataset, conditions, reuse_body) \
        == '(`events.tags[foo]` = 1 OR (groups.b AS `groups.b`) = 2)'

    # Test special output format of LIKE
    conditions = [['events.primary_hash', 'LIKE', '%foo%']]
    assert conditions_expr(dataset, conditions, {}) \
        == '(events.primary_hash AS `events.primary_hash`) LIKE \'%foo%\''

    conditions = tuplify([[['notEmpty', ['arrayElement', ['events.exception_stacks.type', 1]]], '=', 1]])
    assert conditions_expr(dataset, conditions, {}) \
        == 'notEmpty(arrayElement((events.exception_stacks.type AS `events.exception_stacks.type`), 1)) = 1'

    conditions = tuplify([[['notEmpty', ['events.tags[sentry:user]']], '=', 1]])
    assert conditions_expr(dataset, conditions, {}) \
        == 'notEmpty(`events.tags[sentry:user]`) = 1'

    conditions = tuplify([[['notEmpty', ['events.tags_key']], '=', 1]])
    assert conditions_expr(dataset, conditions, {}) \
        == 'notEmpty((arrayJoin(events.tags.key) AS `events.tags_key`)) = 1'

    # Test scalar condition on array column is expanded as an iterator.
    conditions = [['events.exception_frames.filename', 'LIKE', '%foo%']]
    assert conditions_expr(dataset, conditions, {}) \
        == 'arrayExists(x -> assumeNotNull(x LIKE \'%foo%\'), (events.exception_frames.filename AS `events.exception_frames.filename`))'


def test_duplicate_expression_alias():
    dataset = get_dataset("groups")
    state.set_config('use_escape_alias', 1)

    body = {
        'aggregations': [
            ['top3', 'events.logger', 'dupe_alias'],
            ['uniq', 'events.environment', 'dupe_alias'],
        ]
    }
    # In the case where 2 different expressions are aliased
    # to the same thing, one ends up overwriting the other.
    # This may not be ideal as it may mask bugs in query conditions
    exprs = [
        column_expr(dataset, col, body, alias, agg)
        for (agg, col, alias) in body['aggregations']
    ]
    assert exprs == ['(topK(3)(events.logger) AS dupe_alias)', 'dupe_alias']
