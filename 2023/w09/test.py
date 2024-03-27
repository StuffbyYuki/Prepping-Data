import polars as pl
from polars.testing import assert_frame_equal

output = pl.read_csv('data/Output.csv', try_parse_dates=True)
correct_output = pl.read_csv('data/Correct Output.csv', try_parse_dates=True)

if __name__ == '__main__':
    assert_frame_equal(output, correct_output, check_dtype=False, check_row_order=False)