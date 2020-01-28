import pytest

from tabletoolz import *


@pytest.fixture
def response():
    df = car_options >> to_statement >> select([T.option_set_id,T.option_set_price]) >> mutate(markup_price = T.option_set_price * 1.5) >> to_pandas(engine)
    return list(df)

def test_mutate(response):
    # Test calculated column
    query = car_options >> to_statement >> select([T.option_set_id,T.option_set_price]) >> to_pandas(engine)
    query['markup_price'] = query["option_set_price"] * 1.5
    expected = pd.DataFrame(query)
    output = list(expected)
    assert response == output
    
@pytest.fixture
def response():
    df = dealers >> to_statement >> mutate(initials = 'Mr.' + T.dealer_name) >> to_pandas(engine)
    return list(df)

def test_mutate(response):
    # Test calculated column
    query = dealers >> to_statement >> to_pandas(engine)
    query['initials'] = 'Mr.' + query['dealer_name']
    expected = pd.DataFrame(query)
    output = list(expected)
    assert response == output