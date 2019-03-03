from datetime import timedelta
import math

from snuba import state, util


def split_query(query_func):
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

    def wrapper(*args, **kwargs):
        body = args[0]
        use_split, date_align, split_step = state.get_configs(
            [
                ('use_split', 0),
                ('date_align_seconds', 1),
                ('split_step', 3600),  # default 1 hour
            ]
        )
        to_date = util.parse_datetime(body['to_date'], date_align)
        from_date = util.parse_datetime(body['from_date'], date_align)
        limit = body.get('limit', 0)
        remaining_offset = body.get('offset', 0)

        if (
            use_split
            and limit
            and not body.get('groupby')
            and body.get('orderby') == '-timestamp'
        ):
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
                result, status = query_func(*args, **kwargs)

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
                        # If we got nothing from the last query, jump straight to the max time range
                        split_end = split_start
                        split_start = from_date
                    else:
                        # Estimate how big the time range should be for the next query based on
                        # how many results we got for our last query and its time range, and how
                        # many we have left to fetch
                        remaining = limit - total_results
                        split_step = split_step * math.ceil(
                            remaining / float(len(result['data']))
                        )
                        split_end = split_start
                        try:
                            split_start = max(
                                split_end - timedelta(seconds=split_step), from_date
                            )
                        except OverflowError:
                            split_start = from_date
            return overall_result, status
        else:
            return query_func(*args, **kwargs)

    return wrapper
