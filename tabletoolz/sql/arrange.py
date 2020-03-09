from .base import *
from tabletoolz_sql import *,_maybe_wrap
from sqlalchemy import asc, desc
from typing import List
@pipeable
def arrange(columns:List, stmt, ascending=True):
    """
    Function: Sorting is done by the arrange() function, 
              which wraps around the sqlAlchemy.order_by() method. 
    
    Input: A list of columns; intention or strings
    Default argument: ascending=True
    Output: Ordered select statement
    """
    s = _maybe_wrap(stmt)
    cols = [col.evaluate(s) if isinstance(col, Intention) else col for col in columns]
    print(cols)
    if ascending:
        return s.order_by(*cols)
    else:
        cols = [desc(col) for col in cols]
        print(cols)
        return s.order_by(*cols)