import pytest

from tabletoolz import *
from pandas.testing import assert_frame_equal


@pytest.fixture
def response():
    df = car_options >> to_statement >> to_pandas(engine)
    return df

def test_to_statement(response):
    # Test to_statement
    expected = car_options >> to_statement >> to_pandas(engine)    
    assert_frame_equal(response, expected, check_dtype=False)
    
@pytest.fixture
def response_2():
    df = car_options >> to_statement >> pprint
    return str(df)

def test_pprint(response_2):
    # Test pprint of car_options table
    expected = car_options >> to_statement >> pprint
    output = 'SELECT "Car_Options".option_set_id, "Car_Options".model_id, "Car_Options".engine_id, "Car_Options".transmission_id, "Car_Options".chassis_id, "Car_Options".premium_sound_id, "Car_Options".color, "Car_Options".option_set_price \nFROM "Car_Options"'
    assert response_2 == output    
    
@pytest.fixture
def response_3():
    df = car_options >> to_statement >> head(num=5) >> to_pandas(engine)
    return df

def test_head(response_3):
    # Test first 5 rows
    query = car_options >> to_statement >> to_pandas(engine)
    expected = query.head()
    assert_frame_equal(response_3, expected, check_dtype=False)

@pytest.fixture
def response_4():
    df = car_options >> to_statement >> limit(5) >> to_pandas(engine)
    return df

def test_head(response_4):
    # Test first 5 rows
    query = car_options >> to_statement >> to_pandas(engine)
    expected = query.head()
    assert_frame_equal(response_4, expected, check_dtype=False)
    
