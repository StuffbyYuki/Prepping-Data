import polars as pl
from polars.testing import assert_frame_equal

ee_dim_output = pl.read_csv('data/output/ee_dim_output.csv')
ee_monthly_output = pl.read_csv('data/output/ee_monthly_output.csv')
ee_dim_outout_correct = pl.read_csv('data/output/test/ee_dim_v2.csv')
ee_monthly_output_correct = pl.read_csv('data/output/test/ee_monthly_v2.csv')

assert_frame_equal(ee_dim_output, ee_dim_outout_correct, check_row_order=False)