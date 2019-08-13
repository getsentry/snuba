from datetime import timedelta
import math

from snuba import state, util

# Every time we find zero results for a given step, expand the search window by
# this factor. Based on the assumption that the initial window is 2 hours, the
# worst case (there are 0 results in the database) would have us making 4
# queries before hitting the 90d limit (2+20+200+2000 hours == 92 days).
STEP_GROWTH = 10
MIN_COLS = ['project_id', 'event_id', 'timestamp']


def split_query(query_func):
    def wrapper(body, *args, **kwargs):
        use_split = state.get_configs([
            ('use_split', 0),
        ])
        limit = body.get('limit', 0)
        remaining_offset = body.get('offset', 0)
        orderby = util.to_list(body.get('orderby'))

        common_conditions = use_split and limit and not body.get('groupby')

        if common_conditions:
            total_col_count = len(util.all_referenced_columns(body))

            min_col_count = len(util.all_referenced_columns(
                {**body, 'selected_columns': MIN_COLS}))

            if (
                body.get('selected_columns')
                and not body.get('aggregations')
                and total_col_count > min_col_count
            ):
                return col_split(body, *args, **kwargs)
            elif orderby[:1] == ['-timestamp'] and remaining_offset < 1000:
                return time_split(body, *args, **kwargs)

        return query_func(body, *args, **kwargs)

    def time_split(body, *args, **kwargs):
        """
        If a query is:
            - ORDER BY timestamp DESC
            - has no grouping
            - has an offset/limit
            - has a large time range
        We know we have to reverse-sort the entire set of rows to return the small
        chunk at the end of the time range, so optimistically split the time range
        into smaller increments, and start with the last one, so that we can potentially
        avoid querying the entire range.
        """
        date_align, split_step = state.get_configs([
            ('date_align_seconds', 1),
            ('split_step', 3600),  # default 1 hour
        ])

        limit = body.get('limit', 0)
        remaining_offset = body.get('offset', 0)

        to_date = util.parse_datetime(body['to_date'], date_align)
        from_date = util.parse_datetime(body['from_date'], date_align)

        overall_result = None
        split_end = to_date
        split_start = max(split_end - timedelta(seconds=split_step), from_date)
        total_results = 0
        status = 0
        while split_start < split_end and total_results < limit:
            body['from_date'] = split_start.isoformat()
            body['to_date'] = split_end.isoformat()
            # Because its paged, we have to ask for (limit+offset) results
            # and set offset=0 so we can then trim them ourselves.
            body['offset'] = 0
            body['limit'] = limit - total_results + remaining_offset
            result, status = query_func(body, *args, **kwargs)

            # If something failed, discard all progress and just return that
            if status != 200:
                overall_result = result
                break

            if overall_result is None:
                overall_result = result
            else:
                overall_result['data'].extend(result['data'])

            if remaining_offset > 0 and len(overall_result['data']) > 0:
                to_trim = min(remaining_offset, len(overall_result['data']))
                overall_result['data'] = overall_result['data'][to_trim:]
                remaining_offset -= to_trim

            total_results = len(overall_result['data'])

            if total_results < limit:
                if len(result['data']) == 0:
                    # If we got nothing from the last query, expand the range by a static factor
                    split_step = split_step * STEP_GROWTH
                else:
                    # If we got some results but not all of them, estimate how big the time
                    # range should be for the next query based on how many results we got for
                    # our last query and its time range, and how many we have left to fetch.
                    remaining = limit - total_results
                    split_step = split_step * math.ceil(remaining / float(len(result['data'])))

                # Set the start and end of the next query based on the new range.
                split_end = split_start
                try:
                    split_start = max(split_end - timedelta(seconds=split_step), from_date)
                except OverflowError:
                    split_start = from_date

        return overall_result, status

    def col_split(body, *args, **kwargs):
        """
        Split query in 2 steps if a large number of columns is being selected.
            - First query only selects event_id and project_id.
            - Second query selects all fields for only those events.
            - Shrink the date range.
        """
        minimal_query = {**body, 'selected_columns': MIN_COLS}

        result, status = query_func(minimal_query, *args, **kwargs)

        # If something failed, just return
        if status != 200:
            return result, status

        conditions = body.get('conditions', [])

        if result['data']:
            project_ids = list(set([event['project_id'] for event in result['data']]))
            body['project_id'] = project_ids

            event_ids = list(set([event['event_id'] for event in result['data']]))
            conditions.append(('event_id', 'IN', event_ids))

            timestamps = [event['timestamp'] for event in result['data']]
            from_date = util.parse_datetime(min(timestamps))
            # We add 1 second since this gets translated to ('timestamp', '<', to_date)
            # and events are stored with a granularity of 1 second.
            to_date = util.parse_datetime(max(timestamps)) + timedelta(seconds=1)
            body['from_date'] = from_date.isoformat()
            body['to_date'] = to_date.isoformat()
            body['offset'] = 0
            body['limit'] = len(event_ids)

        return query_func({**body, 'conditions': conditions}, *args, **kwargs)

    return wrapper
