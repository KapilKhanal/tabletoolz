from sqlalchemy.sql.selectable import SelectBase, Alias
from sqlalchemy import select as select_sql, func as func_sql, and_, or_
from dfply import make_symbolic, Intention
from ..functoolez import pipeable
from functools import reduce
from sqlparse import format
from sqlalchemy.sql.elements import ColumnClause
from .more_sqlalchemy import everything
import pandas as pd
import sqlalchemy.sql.operators as oops

and_ = make_symbolic(and_)
or_ = make_symbolic(or_)


@pipeable
def to_statement(table_class):
    """Returns all columns from a table as a 'Select' object"""
    return select_sql(everything(table_class))


@pipeable
def pprint(stmt):
    """
    Function: Prints a statement.
    Statements are indented, keywords becomes uppercase.

    Input: A statement
    Output: A "pprinted" statement
    """

    print(format(str(stmt),
           reindent=True,
           keyword_case='upper'))
    return stmt


def _maybe_wrap(stmt):
    if isinstance(stmt, Alias):
        return select_sql([col for col in stmt.c]).select_from(stmt)
    else:
        return stmt


@pipeable
def head(stmt, num = 5):
    """
    Function: Returns the first n rows of statement

    Input: A statement
    Output: n rows of a statement
    """
    s = _maybe_wrap(stmt)
    return s.limit(num)


def _original(stmt):
    return stmt.original if isinstance(stmt, Alias) else stmt

def table_dict(s):
    return {t.name:t for t in _original(s).froms}


D = Intention(function=table_dict)
"""
Access a table using D intention of table name ex: D['heroes']
Access a column using D['heroes'].c.gender
"""
T = Intention(function=lambda s: _original(s).froms[0].c)
"""
References the current table with T intention
"""


def column_dict(stmt):
    s = _maybe_wrap(stmt)
    if len(s.froms) == 1:
        return {col.name:col for col in s.froms[0].c}
    elif len(s.froms) > 1:
        return {t.name + c.name: c
                for t in s.froms
                for c in t.c}
    else:
        raise ValueError('No tables in the from clause')


@pipeable
def get_column(stmt, col, allow_str=True):
    """
    Function: Returns a single column from a statement.
    Column input can be an Intention or a String.

    Input: A statement and a column
    Output: Information about a column
    """
    col_dict = column_dict(stmt)
    if isinstance(col, Intention):
        return col.evaluate(stmt)
    elif allow_str and isinstance(col, str):
        if col in col_dict:
            return col_dict[col]
        else:
            raise ValueError("The column '{0}' not found.".format(col))
    else:
        msg = "Expected " + ("string or " if allow_str else "") + "Intention, got {0}"
        raise ValueError(msg.format(type(col)))


@pipeable
def select(cols, stmt):
    """
    Function: Select variables (columns) that you specify.

    Input: Name of columns, columns have to be in a [list].
    Output: Statement with only selected columns.
    """
    s = _maybe_wrap(stmt)
    return s.with_only_columns([get_column(s, c) for c in cols])


@pipeable
def filter_by(cond, stmt):
    """
    Function: Find rows/cases where conditions are true.

    Input: Logical predicates defined in terms of the variables in a statement.
    Output: A filtered statement.
    """
    s = _maybe_wrap(stmt)
    if isinstance(cond, Intention):
        cond = cond.evaluate(s)
    else:
        "Add msg value error"
    return s.where(cond)


def _kwargs_to_column_expr(stmt, kwargs):
    return  [(col.evaluate(stmt) if isinstance(col, Intention) else col).label(name)
             for name, col in kwargs.items()]


def _add_columns(stmt, kwargs):
    transforms = _kwargs_to_column_expr(stmt, kwargs)
    add_next_column = lambda stmt, col: stmt.column(col)
    return reduce(add_next_column, transforms, stmt)


@pipeable
def mutate(stmt, **kwargs):
    """
    Function: Adds new variables and preserves existing ones

    Input: New variable.
    Output: Statement with new variables.
    """
    s = _maybe_wrap(stmt)
    return _add_columns(s, kwargs).alias() # Need to alias so that subsequent parts of the pipe can reference the new columns


def _col_refs(col_expr):
    col_refs = [str(base_col)
                for c in col_expr.get_children() if isinstance(c, ColumnClause)
                for base_col in c.base_columns]
    return col_refs


def _mutated_refs(mutated_cols, col_expr):
    return len([c for c in _col_refs(col_expr) if c in mutated_cols]) > 0

@pipeable
def mutate_star(stmt, **kwargs):
    mutated_cols = []
    s = _maybe_wrap(stmt)
    for name, col_expr in kwargs.items():
        if _mutated_refs(mutated_cols, col_expr):
            s = s.alias() # current expression refs a previous expr for THIS mutate
            mutated_cols = []
        else:
            mutated_cols.append(name)
        s = _maybe_wrap(s).column(col_expr.label(name))
    return s.alias() # Safest to alias a mutate for easy reference in subsequent calls




summarise = mutate # No functional difference between summarise and mutate


@pipeable
def group_by(cols, stmt):
    """
    Function: Takes an existing statement and converts it into a grouped statements where operations are performed "by group".

    Input: Columns to "group_by". Columns have to be in a [list].
    Output: A grouped statement,
    """
    s = _maybe_wrap(stmt)
    return s.group_by(*[get_column(s, c) for c in cols])



def _sql_func(attr):
    return make_symbolic(getattr(func_sql, attr))

#mean = _sql_func(func.mean) # func.mean is sql alchemy function for mean
# make other functions

class SQLFunction(object):
    def __getattr__(self, attr):
        try:
            return self.__dict__[name] if attr.startswith('__') else _sql_func(attr)
        except KeyError:
            raise AttributeError(name)


func = SQLFunction()


@pipeable
def to_pandas(engine, stmt):
    """
    Function: Converts a SQL statement into a Pandas DataFrame.

    Input: Engine of table.
    Output: A converted Pandas DataFrame.
    """
    return pd.read_sql_query(stmt, con=engine)


@pipeable
def limit(num, stmt):
    """
    Function: Limits n rows that a statement shows

    Input: number of rows to limit.
    Output: Statement with n limited rows.
    """
    s = _maybe_wrap(stmt)
    return s.limit(num)


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

def intersperse(lst, item):
    result = [item] * (len(lst) * 2 - 1)
    result[0::2] = lst
    return result

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
