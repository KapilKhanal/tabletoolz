from .base import *

@pipeable
def unite(columns, colname, stmt, sep='_', remove=True):
    """
    Function: Concatenate columns with a separator. If remove is True, columns are dropped.

    Input: Columns to be concatenated, Name of new column, separator to be separated by.
    Output: Statement with concatenated columns.
    """
    s = _maybe_wrap(stmt)
    cols = [col.evaluate(s) if isinstance(col, Intention) else col for col in columns]
    concat_ready = intersperse(cols, sep)
    united = mutate(s, colname = reduce(lambda acc, nxt: oops.ColumnOperators.concat(acc,nxt), concat_ready))
    return united