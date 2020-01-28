from .base import *

@pipeable
def mutate(stmt, **kwargs):
    """
    Function: Adds new variables and preserves existing ones

    Input: New variable.
    Output: Statement with new variables.
    """
    s = _maybe_wrap(stmt)
    return _add_columns(s, kwargs).alias() # Need to alias so that subsequent parts of the pipe can reference the new columns
