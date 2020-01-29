from .base import *

@pipeable
def select(cols, stmt):
    """
    Function: Select variables (columns) that you specify.

    Input: Name of columns, columns have to be in a [list].
    Output: Statement with only selected columns.
    """
    s = maybe_wrap(stmt)
    return s.with_only_columns([get_column(s, c) for c in cols])
