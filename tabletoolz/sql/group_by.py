from .base import *

@pipeable
def group_by(cols, stmt):
    """
    Function: Takes an existing statement and converts it into a grouped statements where operations are performed "by group".

    Input: Columns to "group_by". Columns have to be in a [list].
    Output: A grouped statement,
    """
    s = _maybe_wrap(stmt)
    return s.group_by(*[get_column(s, c) for c in cols])