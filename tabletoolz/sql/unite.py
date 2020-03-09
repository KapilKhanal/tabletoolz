from .base import *
from tabletoolz.sql import mutate
from drop import drop

@pipeable
def unite(columns, COLNAME, stmt, sep='_', remove=False):
    """
    Function: Concatenate columns with a separator.

    Input: Columns to be concatenated, Name of new column, separator to be separated by.
    Defaults: 
        - False, if remove is set to True, columns are dropped.
        - Seperator defaults to '_'
    Output: Statement with concatenated columns.
    """
    s = maybe_wrap(stmt)
    cols_to_concat = [col.evaluate(s) if isinstance(col, Intention) else col for col in columns]
    concat_ready = intersperse(cols_to_concat, sep)
    united = mutate(s, **{COLNAME:reduce(lambda acc, nxt: oops.ColumnOperators.concat(acc,nxt), concat_ready)})
    if remove:
        cols_to_drop = [col.name for col in cols_to_concat]
        return drop(cols_to_drop,united)
    return united
