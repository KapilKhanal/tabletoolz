import pytest

from tabletoolz import *
from pandas.testing import assert_frame_equal


@pytest.fixture
def response():
    df = car_options >> to_statement >> drop(["color",T.option_set_price]) >> to_pandas(engine)
    return df

def test_drop(response):
    # Multi column drop, Intention and String
    query = car_options >> to_statement >> to_pandas(engine)
    expected = query.drop(['color','option_set_price'],axis=1)
    assert_frame_equal(response, expected, check_dtype=False)
    
@pytest.fixture
def response_2():
    df = car_options >> to_statement >> drop(["color"]) >> to_pandas(engine)
    return df

def test_drop_2(response_2):
    # Single column drop String
    query = car_options >> to_statement >> to_pandas(engine)
    expected = query.drop(['color'],axis=1)
    assert_frame_equal(response_2, expected, check_dtype=False)

def test_drop_exception():
    # Raise error
    with pytest.raises(NameError):
        # Insert any function
        car_options >> to_statement >> drop([color]) 