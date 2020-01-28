import pytest
from tabletoolz import *

@pytest.fixture
def response():
    df = car_options >> to_statement >> filter_by(T.color == 'Blue') >> to_pandas(engine)
    return list(df)

def test_filter_by(response):
    # Test filter, intention
    query = car_options >> to_statement
    stmt = query.where((T.color == 'Blue').evaluate(query))
    expected = to_pandas(engine, stmt)
    output = list(expected)
    assert response == output
    
@pytest.fixture
def response():
    df = car_options >> to_statement >> filter_by("color" == 'Blue') >> to_pandas(engine)
    return list(df)

def test_filter_by(response):
    # Test filter, string
    query = car_options >> to_statement
    stmt = query.where((T.color == 'Blue').evaluate(query))
    expected = to_pandas(engine, stmt)
    output = list(expected)
    assert response == output
    
@pytest.fixture
def response():
    df = car_options >> to_statement >> filter_by(D['Car_Options'].c.option_set_price >= 2000) >> to_pandas(engine)
    return list(df)

def test_filter_by(response):
    # Test predicate and intention
    query = car_options >> to_statement
    stmt = query.where((D['Car_Options'].c.option_set_price >= 2000).evaluate(query))
    expected = to_pandas(engine, stmt)
    output = list(expected)
    assert response == output
    
@pytest.fixture
def response():
    df = dealers >> to_statement >> filter_by(T.dealer_address.contains("Lane")) >> to_pandas(engine)
    return list(df)

def test_filter_by(response):
    # Test contains
    query = dealers >> to_statement
    stmt = query.where((T.dealer_address.contains("Lane")).evaluate(query))
    expected = to_pandas(engine, stmt)
    output = list(expected)
    assert response == output
    
def test_filterby_exception():
    # Test 2 conditions
    with pytest.raises(Exception) as excinfo:
        df = car_options>>to_statement>>filter_by(T.color != "Blue" & T.option_set_price > 1100) >> to_pandas(engine) 
        assert str(excinfo.value) == "__index__ returned non-int (type Intention)"