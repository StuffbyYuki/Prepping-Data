import polars as pl

df = pl.read_csv('data/bank_transactions.csv', try_parse_dates=True)

month_dict = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December"
}

output_df = (
    df
    .with_columns(
        pl.col('Transaction Code').str.split('-').list.get(0).alias('Bank'),
        pl.col('Transaction Date').dt.month().map_dict(month_dict).alias('Transaction Month')
    )
    .group_by('Bank', 'Transaction Month')
    .agg(pl.col('Value').sum())
    .with_columns(
        pl.col('Value').rank("dense", descending=True).over('Transaction Month').alias('Bank Rank per Month')
    )
    .with_columns(
        pl.col('Bank Rank per Month').mean().over('Bank').round(2).alias('Avg Rank per Bank'),
        pl.col('Value').mean().over('Bank Rank per Month').round().cast(pl.Int32).alias('Avg Transaction Value per Rank')
    )
)
print(output_df)