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
def response_2():
    df = car_options >> to_statement >> filter_by("color" == 'Blue') >> to_pandas(engine)
    return list(df)

def test_filter_by_2(response_2):
    # Test filter, string
    query = car_options >> to_statement
    stmt = query.where((T.color == 'Blue').evaluate(query))
    expected = to_pandas(engine, stmt)
    output = list(expected)
    assert response_2 == output
    
@pytest.fixture
def response_3():
    df = car_options >> to_statement >> filter_by(D['Car_Options'].c.option_set_price >= 2000) >> to_pandas(engine)
    return list(df)

def test_filter_by_3(response_3):
    # Test predicate and intention
    query = car_options >> to_statement
    stmt = query.where((D['Car_Options'].c.option_set_price >= 2000).evaluate(query))
    expected = to_pandas(engine, stmt)
    output = list(expected)
    assert response_3 == output
    
@pytest.fixture
def response_4():
    df = dealers >> to_statement >> filter_by(T.dealer_address.contains("Lane")) >> to_pandas(engine)
    return list(df)

def test_filter_by_4(response_4):
    # Test contains
    query = dealers >> to_statement
    stmt = query.where((T.dealer_address.contains("Lane")).evaluate(query))
    expected = to_pandas(engine, stmt)
    output = list(expected)
    assert response_4 == output
    
def test_filterby_exception():
    # Test 2 conditions
    with pytest.raises(Exception) as excinfo:
        df = car_options>>to_statement>>filter_by(T.color != "Blue" & T.option_set_price > 1100) >> to_pandas(engine) 
        assert str(excinfo.value) == "__index__ returned non-int (type Intention)"