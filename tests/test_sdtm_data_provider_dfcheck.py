import pandas as pd
import pytest
import datacompy

from data_providers.sdtm_data_provider import SDTMDataProvider
@pytest.fixture(scope="module")
def dp():
    pass
    # dp = data_provider.DataProvider()
    #dp.clean_slate()   # At times it causes trouble!
    # yield dp

def test_check_dataframes_equal():
    df1 = pd.DataFrame({1: ['a', 'b', 'c'], 2: ['c', 'd', 'e']})
    df2 = pd.DataFrame({1: ['a', 'b', 'c'], 2: ['c', 'd', 'e']})
    expected_result = datacompy.Compare(df1.reset_index(), df2.reset_index(), join_columns=['index'])
    result = SDTMDataProvider.check_dataframes_equal(df1, df2)

    assert expected_result.report() == result
