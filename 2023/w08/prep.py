import polars as pl
import glob

queries = []
for file in glob.glob('data/*.csv'):
    splitted_file_name = file.split('-')
    month = int(splitted_file_name[1].split('.')[0]) if len(splitted_file_name) > 1 else 1
    q = pl.scan_csv(file).with_columns(pl.lit(month).alias('file date'))
    queries.append(q)

df = pl.concat(pl.collect_all(queries))

def clean_market_cap() -> pl.Expr:
    '''
    Convert symbols 'B' and 'M' to zeros while removing '$' symbol
    '''
    conversion = {'M': 1_000_000, 'B': 1_000_000_000}
    col_expr = pl.col('Market Cap').str.replace('$', '', literal=True)
    mutiplier_expr = col_expr.str.slice(-1).replace(conversion).cast(pl.Int64)

    cleaned = (
        pl.when(col_expr.str.contains('B')).then(col_expr.str.replace('B', '', literal=True))
        .when(col_expr.str.contains('M')).then(col_expr.str.replace('M', '', literal=True))
        .otherwise(col_expr)
        .cast(pl.Float64) * mutiplier_expr
    ).cast(pl.Int64)

    return cleaned

df = (
    df
    .filter(pl.col('Market Cap') != 'n/a')
    .with_columns(
        pl.col('Purchase Price').str.replace('$', '', literal=True).cast(pl.Float64),
        clean_market_cap() 
    )
    .with_columns(
        pl.when(pl.col('Purchase Price') < 25_000).then(pl.lit('Low'))
        .when(pl.col('Purchase Price') < 50_000).then(pl.lit('Medium'))
        .when(pl.col('Purchase Price') < 75_000).then(pl.lit('High'))
        .when(pl.col('Purchase Price') < 100_000).then(pl.lit('Very High'))
        .alias('Purchase Price Category'),
        pl.when(pl.col('Market Cap') < 100_000_000).then(pl.lit('Small'))
        .when(pl.col('Market Cap') < 1_000_000_000).then(pl.lit('Medium'))
        .when(pl.col('Market Cap') < 100_000_000_000).then(pl.lit('Large'))
        .when(pl.col('Market Cap') >= 100_000_000_000).then(pl.lit('Huge'))
        .alias('Market Cap Category')
    )
    .with_columns(
        pl.col('Purchase Price')
        .rank('ordinal', descending=True).over('file date', 'Purchase Price Category', 'Market Cap Category')
        .alias('Rank')
    )
    .filter(pl.col('Rank')<=5)
)

print(df.head())



