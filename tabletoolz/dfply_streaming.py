from itertools import chain
from operator import and_, or_, not_
from dfply import make_symbolic, Intention
from dfply.base import _set_magic_method
from collections import OrderedDict
from toolz import curry

setattr(Intention, '__invert__', _set_magic_method('__invert__'))

X = Intention()


EXAMPLE_VALUES = {int:2, str:'a', float:2.5}

def merge(d1, d2):
    return dict(chain(d1.items(), d2.items()))
    

def extract_type(type_tuple):
    return tuple(t for t in type_tuple if t is not type(None))[0]
#Automatic type inference?

def extract_types(row):
    return {c:extract_type(f.type) 
            for c, f in row._precord_fields.items() 
            if c not in ('columns', 'col_types')}


def get_example_row(row):
    return {col:EXAMPLE_VALUES[t] for col, t in extract_types(row).items()}


def get_new_types(raw_output):
    return {col:type(val) for col, val in raw_output.items() if col not in ('columns', 'col_types')}


def same_types(old_types, new_types):
    assert len(old_types) == len(new_types) and all(col in old_types for col in new_types)
    return all(old_types[col] == new_types[col] for col in new_types)


def row_values(row):
    return {col:val for col, val in row.items() if col not in ('columns', 'col_types')}

@curry
def convert_to_rows(seq, col_types = {}):
    iseq = iter(seq)
    first = next(iseq)
    assert isinstance(first, OrderedDict), "To preserve column order, use an OrderDict or DictReader"
    cols = tuple(first.keys())
    all_col_types = merge({c:str for c in cols}, col_types)
    Row = make_row_class(cols, all_col_types)
    base_row = Row(**{'columns':cols, 'col_types':all_col_types})
    yield base_row.set(**first)
    for raw_row in iseq:
        yield base_row.set(**raw_row)
        


def apply_row_func(row_func, seq, col_types = {}):
    iseq = iter(seq)
    first = next(iseq)
    old_cols = tuple(first.columns.all_columns)
    old_col_types = first.col_types
    example_row = first.set(**get_example_row(first))
    example_output = row_func(example_row)
    new_types = get_new_types(example_output)
    if set(old_cols) == set(example_row.keys()) and same_types(old_col_types, new_types):
        # Columns not have changed and no new types.  Can use the old row type.
        row_cls = first.__class__
    else:
        # Need to maintain the order
        old_cols = tuple(col for col in example_output if col in old_cols)
        new_cols = tuple(col for col in example_output if col not in old_cols)
        all_cols = old_cols + new_cols
        row_cls = make_row_class(all_cols, new_types) # Disregarding the kwargs for now
    base_row = row_cls(**{'columns':all_cols, 'col_types':new_types})
    yield base_row.set(**row_func(first))
    for raw_row in iseq:
        yield base_row.set(**row_func(raw_row))
        



def wrap_row_intention(row_func):
    if isinstance(row_func, Intention):
        return lambda row: row_func.evaluate(row)
    else:
        return row_func
    

def maybe_apply(func):
    def _wrapped(val):
        if val is None:
            return None
        try:
            output = func(val)
            # HACK!! For some reason Intention arithmetic on NoneType returns NotImplemented
            # Need to track this down and/or thoroughly test
            return None if output == NotImplemented else output
        except TypeError as e:
            if 'NoneType' in str(e):
                return None
            else:
                raise e
    return _wrapped

    
def wrap_kwargs(kwargs):
    return {key: maybe_apply(wrap_row_intention(f)) for key, f in kwargs.items()}


@curry
def mutate_row(row, **kwargs): #func?
    new_values = {col:func(row) for col, func in kwargs.items() if col not in ('columns', 'col_types')}
    old_values = {col:row for col, row in row.items() if col not in ('columns', 'col_types')}
    return merge(old_values, new_values)


@curry
def mutate(df, col_types = {}, **kwargs):
    kwargs = wrap_kwargs(kwargs)
    yield from apply_row_func(mutate_row(**kwargs), df, col_types= col_types)

    
def wrap_col_intention(columns):
    if isinstance(columns, Intention):
        return lambda row: columns.evaluate(row.columns).current_set
    elif hasattr(columns, 'current_set'):
        return lambda row: columns.current_set
    else:
        return lambda row: pset(columns)

    
@curry
def select_row(col_selection_func, row):
    return {col:val for col, val in row.items() if col in col_selection_func(row)}


@curry
def select(columns, df):
    cols = wrap_col_intention(columns)
    col_select_func = lambda row: {col:val for col, val in row.items() if col in cols(row)}
    yield from apply_row_func(col_select_func, df)


# Operators for intentions
and_ = make_symbolic(and_)
or_ = make_symbolic(or_)
not_ = make_symbolic(not_)

def maybe_pred_intent(intention):
    def _wrapped(val):
        try:
            return intention.evaluate(val)
        except TypeError as e:
            if 'NoneType' in str(e) or 'NotImplementedType' in str(e):
                return False
            else:
                raise e
    return _wrapped
            

def maybe_map_pred(pred_func):
    maybe_pred = maybe_apply(pred_func)
    def _wrapped(row):
        try:
            output = maybe_pred(row)
            return output if isinstance(output, bool) else False
        except:
            return False
    return _wrapped

@curry
def filter_by(pred, df):
    if isinstance(pred, Intention):
        pred_func = maybe_pred_intent(pred)
    elif callable(pred):
        pred_func = maybe_map_pred(pred)
    else:
        raise TypeError("The first argument of filter_by needs to be an Intension or predicate function")
    yield from filter(pred_func, df)

# %load columns.py
from pyrsistent import pmap, pset, pset_field, pvector_field, PRecord, field

__all__ = ["Columns",
           "make_columns"]

_COMPARISON_METHODS = ['__ne__',
                       '__ge__', 
                       '__lt__', 
                       '__eq__', 
                       '__gt__', 
                       'issuperset', 
                       'issubset',
                       '__contains__', 
                       'isdisjoint',
                       '__le__']

_UNARY_OPERATIONS = ['__len__', 
                     '__sizeof__'] 

_SET_OPERATIONS = ['__and__', 
                   '__sub__', 
                   '__or__',  
                   '__xor__', 
                   'union', 
                   'intersection', 
                   'difference', 
                   'symmetric_difference', 
                   'update']


def wrap_comparison_method(method_name):
    def magic_method(self, other):
        if isinstance(other, PRecord) and hasattr(other, 'current_set'): 
            return getattr(self.current_set, method_name)(other.current_set)
        else:
            return getattr(self.current_set, method_name)(other)
    return magic_method


def wrap_unary_method(method_name):
    def magic_method(self):
        return getattr(self.current_set, method_name)()
    return magic_method


def wrap_set_operations(method_name):
    def magic_method(self, other):
        if isinstance(other, PRecord) and hasattr(other, 'current_set'): 
            current_set = getattr(self.current_set, method_name)(other.current_set)
        else:
            current_set = getattr(self.current_set, method_name)(other)
        return self.__class__(all_columns = self.all_columns, current_set=current_set)
    return magic_method


def column_invert(self):
    current_set = pset(self.all_columns) - self.current_set
    return self.__class__(all_columns=self.all_columns, current_set=current_set)


class Columns(PRecord):
    all_columns = pvector_field(str)
    current_set = pset_field(str)
    
    
for method in _COMPARISON_METHODS:
    setattr(Columns, method, wrap_comparison_method(method))

for method in _UNARY_OPERATIONS:
    setattr(Columns, method, wrap_unary_method(method))
    
for method in _SET_OPERATIONS:
    setattr(Columns, method, wrap_set_operations(method))
    
setattr(Columns, '__invert__', column_invert)
setattr(Columns, '__iter__', lambda self: self.current_set.__iter__())



def _make_unique_column_name():
    n = 0
    def col_maker():
        nonlocal n
        n += 1
        return 'Column' + str(n)
    return col_maker


_col_name = _make_unique_column_name()


def _column_fields(columns):
    return {n:field(factory=lambda s: Columns(all_columns=columns, current_set=pset([s]))) 
            for n in columns if n.isidentifier()}


def _make_column_type(columns):
    return type(_col_name(), (Columns, ), _column_fields(columns))


def _make_columns_input(columns):
    output = pmap({'all_columns':columns,
                   'current_set':columns})
    return output.update({n:n for n in columns})


def make_columns(columns):
    return _make_column_type(columns)(**_make_columns_input(columns))

# %load row.py
from pyrsistent import PRecord, field, freeze, pmap
#
### TODO: Uncomment when splitting these apart
#
#from columns import make_columns

class Row(PRecord):
    """Base class for the rows in a data frame.  
    
       This class will be used to dynamically create new classes for rows, which
       have properties for each column.  By inheriting from PRecord, we can leverage 
       the following functionality.
       
       1. types and factories: We will be able to create factories and types to easily handle column types.
       2. Easy interaction with Intentions.  For example, X.col1 will evaluate to the value of col1.
       3. COMING SOON: Define invariants for columns (e.g. must be positive or between 0 and 1)
       
       """
    columns = field(factory = make_columns)
    col_types = field()
    verbose = False
    
    
    def __repr__(self):
        names = self.columns.all_columns
        cols = (", ".join(["{0} = {1}".format(col, repr(self[col])) for col in names]), )
        if self.verbose:
            col_str = ("columns = [" + ", ".join(["{0}".format(n) for n in names]) + "],",)
            type_str = ("col_types = {" + " ".join(["{0} = {1},".format(n, str(t)) for n, t in self.col_types.items()]) + "}",)
        else:
            col_str = type_str = tuple([])
        return "Row(" + "\n    ".join(cols + col_str + type_str) + ")"
    
    
    
def col_factory(type_constructor):
    if type_constructor.__name__ == 'str':
        return lambda val: str(val) if val is not None and len(str(val)) > 0 else None
    else:
        def factory(val):
            if val is None:
                return None
            elif isinstance(val, type_constructor):
                return val
            else:
                try:
                    return type_constructor(val)
                except:
                    return None
        return factory
    

def get_col_types(names, col_type_dict):
    return [col_type_dict.get(name, str) for name in names]


def get_field(col_type, **kwargs):
    return field(type=(col_type, type(None)),
                 factory = col_factory(col_type),
                 initial = None,
                 **kwargs)

    
def columns_and_types(names, col_type_dict):
    return zip(names, get_col_types(names, col_type_dict))


def row_fields(names, col_type_dict, kwarg_dict={}):
    return {name:get_field(col_type, **kwarg_dict.get(name, {})) 
            for name, col_type in columns_and_types(names, col_type_dict) 
            if name.isidentifier()}
    

def _make_unique_name(base):
    n = 0
    def col_maker():
        nonlocal n
        n += 1
        return base + str(n)
    return col_maker


_col_name = _make_unique_name("Column")
_row_name = _make_unique_name("Row")


def make_row_class(names, col_type_dict, field_kwargs = {}):
    return type(_row_name(), (Row,), row_fields(names, col_type_dict, kwarg_dict=field_kwargs))


def extract_column_types(row):
    """Extracts the assigned types from a row"""
    return {col:v.type.difference(set([type(None)])).pop() for col, v in row._precord_fields.items()}

