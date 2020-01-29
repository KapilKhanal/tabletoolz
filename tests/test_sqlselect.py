import pytest

from tabletoolz import *

# Test case function
def test_cases(col):
    df = car_options >> to_statement >> select(col) >> to_pandas(engine)
    return list(df)

@pytest.fixture
def response():
    df = car_options >> to_statement >> select([T.option_set_id]) >> to_pandas(engine)
    return list(df)

def test_select(response):
    # Single intention 
    query = car_options >> to_statement >> to_pandas(engine)
    expected = query[['option_set_id']]
    output = list(expected)
    assert response == output
    
@pytest.fixture
def response():
    df = car_options >> to_statement >> select(["option_set_id"]) >> to_pandas(engine)
    return list(df)

def test_select(response):
    # Single string column 
    query = car_options >> to_statement >> to_pandas(engine)
    expected = query[['option_set_id']]
    output = list(expected)
    assert response == output
    
def test_exception():
    # Empty list
    with pytest.raises(Exception) as excinfo:
        test_cases([]) 
        assert str(excinfo.value) == "incomplete input"
        

def test_exception():
    with pytest.raises(Exception) as excinfo:
        # Test intention, Raises TypeError
        test_cases(T.option_set_id)
        assert str(excinfo.value) == "iter() returned non-iterator of type 'Intention'"
        
@pytest.fixture
def response():
    df = car_options >> to_statement >> select([T.option_set_id, "model_id", D["Car_Options"].c.color]) >> to_pandas(engine)
    return list(df)

def test_select(response):
    # Multiple columns, mixed types
    query = car_options >> to_statement >> to_pandas(engine)
    expected = query[['option_set_id', 'model_id', 'color']]
    output = list(expected)
    assert response == output

def test_exception():
    with pytest.raises(Exception) as excinfo:
        # Single column, string
        test_cases(option_set_id)
        assert str(excinfo.value) == "name 'option_set_id' is not defined"