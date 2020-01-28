from .base import *

@pipeable
def rename(stmt, **kwargs):
    """
    Function: Renames variables (columns) from the statement.

    Input: A list of columns and its new name.
    Output: Statement with renamed columns.
    """
    s = _maybe_wrap(stmt)
    s = _add_columns(s, kwargs).alias()
    s = drop(kwargs.values(), s)
    return s