import polars as pl
from polars.testing import assert_frame_equal
import sys
sys.path.append('..')
print(sys.path)


def test_dim_output():
    ee_dim_output = pl.read_csv('data/output/ee_dim_output.csv')
    ee_dim_outout_expected = pl.read_csv('tests/ee_dim_v2.csv')
    assert_frame_equal(ee_dim_output, ee_dim_outout_expected, check_row_order=False, check_column_order=False)

def test_monthly_output():
    ee_monthly_output = pl.read_csv('data/output/ee_monthly_output.csv')
    ee_monthly_output_expected = pl.read_csv('tests/ee_monthly_v2.csv')
    assert_frame_equal(ee_monthly_output, ee_monthly_output_expected, check_row_order=False, check_column_order=False)
