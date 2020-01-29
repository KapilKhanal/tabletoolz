from inspect import signature
from functools import reduce
from toolz import first, drop
from toolz import curry
from toolz.functoolz import Compose


class pipeable(curry):


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    def __rrshift__(self, other):
        """ We use the rightshift (i.e. `x >> f`) to represent piping.

        Note that this form or piping assumes a unary function call.
        Use a curried function to allow piping n-ary functions."""
        return self.__call__(other)


    def __rrshift__(self, other):
        """ We use the rightshift (i.e. `x >> f`) to represent piping.

        Note that this form or piping assumes a unary function call.
        Use a curried function to allow piping n-ary functions."""
        return self.__call__(other)


    def __rshift__(self, other):
        """ We use the rightshift (i.e. `x >> f`) to represent piping.

        Note that this form or piping assumes a unary function call.
        Use a curried function to allow piping n-ary functions."""
        assert callable(other), "All subsequent elements of a pipe must be callable"
        return other.__call__(self)

def make_pipeable(*args):
    """ Creates 1 or more pipable functions

    >>> roundp, maxp = make_pipeable(round, max)
    >>> maxp(range(10)) >> roundp(digits = 2)"""
    num_funcs = len(args)
    assert num_funcs > 0 and all(callable(f) for f in args), "You must input 1 or more functions"
    return pipeable(first(args)) if len(args) == 1 else tuple(pipeable(f) for f in args)

@pipeable
def my_add(x, y):
    return x+y

class Pipe(object):

    def __init__(self, value=None):
        self.value = value

    def __rrshift__(self, other):
        return Pipe(other)

    def __rshift__(self, other):
        assert other is not None, "A pipe needs to start with a value" 
        assert callable(other), "A pipe needs to be proceeded by a function"
        return other(self.value)

# Various aliases for the pipe
p = Pipe()
pipe = Pipe()
pype = Pipe()


class composable(Compose):
    """ Create a composable object which allows composition with <<

    Note that funcs needs to be in reverse order, i.e. funcs = (h,g,f)
    corresponds to compose(f,g,h).  This should not come up in practice,
    instead use new_f = f << g << h"""

    def __init__(self, funcs):
        funcs = (funcs, ) if callable(funcs) else funcs
        super().__init__(funcs)

    def __rlshift__(self, other):
        """ f << g_comp returns a composable object 
        
        This is equivalent to compose(f,g_comp)"""
        assert callable(other), "the composition operator needs a callable object, got {0}".format(type(other))
        new_funcs = (other,) + self.funcs
        return composable(new_funcs)

    def __lshift__(self, other):
        """ f_comp << g returns a composable object equivalent to compose(f_comp,g)"""
        assert callable(other), "the composition operator needs a callable object, got {0}".format(type(other))
        new_funcs = self.funcs + (other,)
        return composable(new_funcs)



def make_composable(*args):
    """ Use to create 1 or more composable functions

    >>> round_2 = lambda num: round(num, 2)
    >>> round_comp, max_comp = make_composable(round_2, max)
    >>> f = round_comp << max_comp"""
    return tuple(composable(f) for f in args)


@composable
def comp_add(x):
    return x + 2

@composable
def comp_mult(x, y):
    return x*y

class Compose(object):
    def __init__(self):
        pass

    def __lshift__(self, other):
        assert callable(other), "Can only compose functions/callable objects"
        return composable(other)


compose = Compose()
