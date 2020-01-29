import pytest

from tabletoolz import *
from pandas.testing import assert_frame_equal

@pytest.fixture
def response():
    df = car_options >> to_statement >> rename(COLOUR=T.color, PRICE=T.option_set_price) >> to_pandas(engine)
    return df

def test_rename(response):
    # Test renaming columns
    query = car_options >> to_statement >> to_pandas(engine)
    expected = query.rename(columns={"color": "COLOUR", "option_set_price": "PRICE"})
    assert_frame_equal(response, expected, check_dtype=False)

def test_rename_exception():
    # String columns
    with pytest.raises(Exception) as excinfo:
        car_options >> to_statement >> rename(COLOUR= "color")
        assert str(excinfo.value) == "str' object has no attribute 'label'"
