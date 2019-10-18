from sqlalchemy.sql.selectable import SelectBase, Alias
from sqlalchemy import select as select_sql, func as func_sql, and_, or_
from dfply import make_symbolic, Intention
from functoolz import pipeable
from functools import reduce
from sqlparse import format
from sqlalchemy.sql.elements import ColumnClause
from more_sqlalchemy import everything
import pandas as pd

and_ = make_symbolic(and_)
or_ = make_symbolic(or_)


@pipeable
def to_statement(table_class):
    """Returns all columns from a table as a 'Select' object"""
    
    return select_sql(everything(table_class))


@pipeable
def pprint(stmt):
    """Prints a statement,
    
    Indentations of the statement are adjusted
    Keywords becomes uppercase
    """
    
    print(format(str(stmt),  
           reindent=True, 
           keyword_case='upper'))
    return stmt


def _maybe_wrap(stmt):
    if isinstance(stmt, Alias):
        return select_sql(everything(stmt)).select_from(stmt)
    else:
        return stmt


@pipeable
def head(stmt, num = 5):
    s = _maybe_wrap(stmt)
    return s.limit(num)


def _original(stmt):
    return stmt.original if isinstance(stmt, Alias) else stmt

def table_dict(s): 
    return {t.name:t for t in _original(s).froms}


D = Intention(function=table_dict)
T = Intention(function=lambda s: _original(s).froms[0].c)


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
    s = _maybe_wrap(stmt)
    return s.with_only_columns([get_column(s, c) for c in cols])


@pipeable
def filter_by(cond, stmt):
    s = _maybe_wrap(stmt)
    if isinstance(cond, Intention):
        cond = cond.evaluate(s)
    return s.where(cond)


def _kwargs_to_column_expr(stmt, kwargs):
    return  [(col.evaluate(stmt) if isinstance(col, Intention) else col).label(name)
             for name, col in kwargs.items()]


def _add_columns(stmt, kwargs):
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
    s = _maybe_wrap(stmt)
    for name, col_expr in kwargs.items():
        if _mutated_refs(mutated_cols, col_expr):
            s = s.alias() # current expression refs a previous expr for THIS mutate
            mutated_cols = []
        else:
            mutated_cols.append(name)
        s = _maybe_wrap(s).column(col_expr.label(name))
    return s.alias() # Safest to alias a mutate for easy reference in subsequent calls
    

@pipeable
def mutate(stmt, **kwargs):
    s = _maybe_wrap(stmt)
    return _add_columns(s, kwargs).alias() # Need to alias so that subsequent parts of the pipe can reference the new columns
    
    
summarise = mutate # No functional difference between summarise and mutate


@pipeable
def group_by(cols, stmt):
    s = _maybe_wrap(stmt)
    return s.group_by(*[get_column(s, c) for c in cols])


def _sql_func(attr):
    return make_symbolic(getattr(func_sql, attr))
                             
                             
class SQLFunction(object):
    def __getattr__(self, attr):
        try:
            return self.__dict__[name] if attr.startswith('__') else _sql_func(attr)
        except KeyError:
            raise AttributeError(name)
                                     
                        
func = SQLFunction()


@pipeable
def to_pandas(engine, stmt):
    return pd.read_sql_query(stmt, con=engine)


@pipeable
def limit(num, stmt):
    s = _maybe_wrap(stmt)
    return s.limit(num)



