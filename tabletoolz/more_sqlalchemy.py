from sqlalchemy import Integer, Float, String, DateTime
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.elements import _interpret_as_column_or_from
from sqlalchemy.sql.annotation import AnnotatedColumn
from sqlalchemy import column
from functoolz import pipeable
from sqlparse import format

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



