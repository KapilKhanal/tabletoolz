from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.selectable import SelectBase, Alias, Select
from sqlalchemy.sql.elements import _interpret_as_column_or_from, BinaryExpression, BindParameter, BooleanClauseList, ColumnClause
from sqlalchemy.sql.annotation import AnnotatedColumn
from sqlalchemy.sql import operators
import sqlalchemy.sql.operators as oops
from sqlalchemy import select as select_sql, func as func_sql, and_, or_
from sqlalchemy import join as join_sql
from sqlalchemy import Integer, Float, String, DateTime, column
from sqlparse import format
from dfply import make_symbolic, Intention
from ..functoolez import pipeable
from functools import reduce
import pandas as pd
import operator




DTYPES_TO_SQLALCHEMY_TYPES = {'O':String,
                              'i':Integer,
                              'f':Float,
                              'M':DateTime}


pprint = lambda stmt: print(format(str(stmt),
                             reindent=True,
                             keyword_case='upper'))


def get_sql_types(df):
    sql_type = lambda dtype: DTYPES_TO_SQLALCHEMY_TYPES[dtype.kind]
    cols_and_dtypes = lambda df: zip(df.columns, df.dtypes)
    return {col:sql_type(dtype)
            for col, dtype in cols_and_dtypes(df)}


result_dict = lambda r: dict(zip(r.keys(), r))
result_dicts = pipeable(lambda rs: list(map(result_dict, rs)))
check_unique = pipeable(lambda df: [(col, df[col].is_unique) for col in df.columns])



def get_column_name(column):
    if isinstance(column, str):
        name = column
    elif type(column) == InstrumentedAttribute and type(column) == AnnotatedColumn:
        name = column.expression.name
    else:
        raise TypeError("column needs to be a string or table class attribute")
    return name


def col_selector(col, from_=None, to=None, inclusive=True):
    col_names = [c.name for c in col.table.columns]
    from_idx, to_idx = 0, len(col_names)
    if from_:
        from_idx = col_names.index(from_) if inclusive else col_names.index(from_) + 1
    if to:
        to_idx = col_names.index(to) if not inclusive else col_names.index(to) + 1
    return col_names[from_idx:to_idx]


def get_column_list(col, from_=None, to=None, inclusive=True):
    selected_cols = col_selector(col, from_=from_, to=to, inclusive=inclusive)
    return [c for c in col.table.columns if c.name in selected_cols]


def get_filtered_columns(filter_columns, table, substr):
    return [c for c in table.columns if filter_columns(c, substr)]

filt_startswith = lambda c, substr: c.name.startswith(substr)
filt_endswith = lambda c, substr: c.name.endswith(substr)
filt_contains = lambda c, substr: substr in c.name


def col_startswith(table_class, prefix):
    return  get_filtered_columns(filt_startswith, table_class.__table__, prefix)


def col_endswith(table_class, suffix):
    return  get_filtered_columns(filt_endswith, table_class.__table__, suffix)


def col_contains(table_class, substr):
    return  get_filtered_columns(filt_contains, table_class.__table__, substr)


def everything(table_class):
    return [c for c in table_class.__table__.columns]


def all_but(cols):
    dropped_cols = [_interpret_as_column_or_from(col) for col in cols]
    first_col = dropped_cols[0]
    if not all(col.table.name == first_col.table.name for col in dropped_cols):
        raise ValueError('all_but arguments need to all be from the same table')
    dropped_names = [col.name for col in  dropped_cols]
    return [c for c in first_col.table.columns if c.name not in dropped_names]


def cols_from(col, inclusive=True):
    c = _interpret_as_column_or_from(col)
    return  get_column_list(c, from_=c.name, inclusive=inclusive)


def cols_to(col, inclusive=True):
    c = _interpret_as_column_or_from(col)
    return  get_column_list(c, to=c.name, inclusive=inclusive)


def cols_between(col1, col2, inclusive=True):
    c1 = _interpret_as_column_or_from(col1)
    c2 = _interpret_as_column_or_from(col2)
    if c1.table.name != c2.table.name:
        raise ValueError("cols_between needs both columns to be from the same table")
    return get_column_list(c1, from_=c1.name, to=c2.name, inclusive=inclusive)

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

def maybe_wrap(stmt):
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
    s = maybe_wrap(stmt)
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
    s = maybe_wrap(stmt)
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
def filter_by(cond, stmt):
    """
    Function: Find rows/cases where conditions are true.

    Input: Logical predicates defined in terms of the variables in a statement.
    Output: A filtered statement.
    """
    s = maybe_wrap(stmt)
    if isinstance(cond, Intention):
        cond = cond.evaluate(s)
    else:
        "Add msg value error"
    return s.where(cond)

def _kwargs_to_column_expr(stmt, kwargs):
    return  [(col.evaluate(stmt) if isinstance(col, Intention) else col).label(name)
             for name, col in kwargs.items()]


def add_columns(stmt, kwargs):
    transforms = _kwargs_to_column_expr(stmt, kwargs)
    add_next_column = lambda stmt, col: stmt.column(col)
    return reduce(add_next_column, transforms, stmt)

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
    s = maybe_wrap(stmt)
    for name, col_expr in kwargs.items():
        if _mutated_refs(mutated_cols, col_expr):
            s = s.alias() # current expression refs a previous expr for THIS mutate
            mutated_cols = []
        else:
            mutated_cols.append(name)
        s = maybe_wrap(s).column(col_expr.label(name))
    return s.alias() # Safest to alias a mutate for easy reference in subsequent calls

#summarise = mutate # No functional difference between summarise and mutate

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
    s = maybe_wrap(stmt)
    return s.limit(num)

def intersperse(lst, item):
    result = [item] * (len(lst) * 2 - 1)
    result[0::2] = lst
    return result

def sql_func(attr):
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

def get_onclause_col(criterion):
    left = []
    right = []
    if isinstance(criterion, BooleanClauseList):
        for clause in criterion.clauses:
            if isinstance(clause, BinaryExpression):
                left.append(clause.left)
                right.append(clause.right)
        return (left,right)

def table_check(table):
    if isinstance(table, Select):
        table = table.alias()
        return table
    else:
        table = selectq(everything(table)).alias()
        return table


@pipeable
def inner_join(right, left, onclause):
    """
    Function: Inner Join joins on conditions 
    
    Input: 
        1. Right statement/table to be joined, 
        2. Left statement/table to be joined,  
        3. A SQL expression representing the ON clause of the join. If left at None, it attempts to join the two tables based on a foreign key relationship
    
    If more than 1 expression is listed, (expressions) have to use an '&' operator. Ex: (Table1.c.colname1 = Table2.c.colname1 & Table1.c.colname2 = Table2.c.colname2)
    
    Output: A joined object
    
    Example: Table1 >> to_statement >> inner_join(Table2, onclause=(Table1.c.colname = Table2.c.colname):BooleanClauseList)
    """
    left, right = table_check(left), table_check(right)
    l1,r1 = get_onclause_col(onclause)
    l2, r2 = [c.name for c in l1], [c.name for c in r1]
    l3, r3 = sorted([c for c in left.columns if c.name in l2], key= lambda x:x.name), sorted([c for c in right.columns if c.name in r2], key=lambda x:x.name)
    col_dict = dict(zip(l3, r3))
    expr_list  = [(k==v) for k,v in col_dict.items()]
    clause = reduce(lambda x,y : operator.iand(x, y),expr_list)
    return select_sql([left.join_sql(right, onclause=clause)])

@pipeable
def left_join(right, left, onclause):
    """
    Function: Join columns using an inner join
    
    Input: 
        1. Right statement/table to be joined, 
        2. Left statement/table to be joined,  
        3. A SQL expression representing the ON clause of the join. If left at None, it attempts to join the two tables based on a foreign key relationship
    
    If more than 1 expression is listed, (expressions) have to use an '&' operator. Ex: (Table1.c.colname1 = Table2.c.colname1 & Table1.c.colname2 = Table2.c.colname2)
    
    Output: A joined object
    
    Example: Table1 >> to_statement >> left_join(Table2, onclause=(Table1.c.colname = Table2.c.colname):BooleanClauseList)
    """
    left, right = left.alias(), table_check(right)
    print(left,right)
    l1,r1 = get_onclause_col(onclause)
    l2, r2 = [c.name for c in l1], [c.name for c in r1]
    l3, r3 = sorted([c for c in left.columns if c.name in l2], key= lambda x:x.name), sorted([c for c in right.columns if c.name in r2], key=lambda x:x.name)
    col_dict = dict(zip(l3, r3))
    expr_list  = [(k==v) for k,v in col_dict.items()]
    clause = reduce(lambda x,y : operator.iand(x, y),expr_list)
    return select_sql([left.join_sql(right, onclause=clause, isouter=True)])

