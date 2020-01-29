import pytest

from tabletoolz import *
from pandas.testing import assert_frame_equal


Mean = sql_func('avg')

@pytest.fixture
def response():
    df = models >> to_statement >> group_by([T.brand_id]) >> mutate(model_base_prices = Mean(T.model_base_price)) >> select([T.brand_id,'model_base_prices']) >> to_pandas(engine)
    return df

def test_groupby(response):
    # Test single column operation
    query = models >> to_statement >> to_pandas(engine)
    Grouped = query.groupby(['brand_id'], as_index=False).mean()
    Grouped['model_base_prices'] = Grouped["model_base_price"]
    Grouped = Grouped[['brand_id','model_base_prices']]
    expected = pd.DataFrame(Grouped)
    assert_frame_equal(response, expected, check_dtype=False)