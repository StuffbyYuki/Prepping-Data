import polars as pl
from datetime import date

FILE_PATH = 'AllChains Data.csv'
MAX_DATE = date(2023, 6, 30)
MAX_MONTH_FIRST_DATE = date(2023, 6, 1)


df = pl.read_csv(FILE_PATH)

intermediate = (
    df
    .with_columns(
        pl.col('Date').str.to_date('%d/%m/%Y'),
    )
    .filter(pl.col('Date').le(MAX_DATE))
    .groupby_dynamic('Date', every='1mo', by=['Store', 'Bike Type']).agg(
        pl.col('Sales').sum(), 
        pl.col('Profit').sum()
    )
)

supplimental_rows = (
    intermediate
    .with_columns(
        pl.col('Date').max().over(['Store', 'Bike Type']).alias('max month per group')
    )
    .filter(
        (pl.col('max month per group')!=MAX_MONTH_FIRST_DATE)
        &
        (pl.col('max month per group')==pl.col('Date'))
    )
    .select(
        'Store',
        'Bike Type',
        pl.lit(MAX_MONTH_FIRST_DATE).alias('Date'),
        pl.lit(0.0).alias('Sales'),
        pl.lit(0.0).alias('Profit')
    )
)


result = (
    intermediate
    .vstack(supplimental_rows)
    .sort('Store', 'Bike Type', 'Date')
    .set_sorted(['Date', 'Store', 'Bike Type'])
    .upsample('Date', every='1mo', by=['Store', 'Bike Type'])  # filling dates where there is no sales value
    .with_columns(
        pl.col(['Store', 'Bike Type']).fill_null(strategy='forward'),
        pl.col(['Sales', 'Profit']).fill_null(0)
    )
    .select(
        pl.col('Date').alias('Month'),
        pl.col('Store'),
        pl.col('Bike Type'),
        pl.col('Sales').round(2),
        pl.col('Profit').round(2),
        pl.col('Profit').rolling_mean(window_size=3).over(['Store', 'Bike Type']).round(2).alias('3 Month Moving Average Profit')
    )
    .sort(['Store', 'Bike Type', 'Month'])
)

print(result)
