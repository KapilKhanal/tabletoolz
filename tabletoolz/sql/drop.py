from .base import *
from .tabletoolz_sql import *
#from .select import *
@pipeable
def drop(cols, stmt):
    """
    Function: Drops columns from the statement.

    Input: List of columns to drop.
    Output: Statement with columns that are not dropped.
    """
    col_dict = column_dict(stmt)
    col_names = [c for c in col_dict.keys()]
    colintention = [c.evaluate(stmt).name if isinstance(c, Intention) else c for c in cols]
    new_cols = list(filter(lambda c: c not in colintention, col_names))
    undrop = select(new_cols, stmt)
    return undrop
