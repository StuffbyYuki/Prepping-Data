import polars as pl
from custom_expressions import CustomStringMethodsCollection

# set to show more rows when print
pl.Config.set_tbl_rows(20)

# read data
file_name = 'Messy Nut House Data.csv'
df = pl.scan_csv(file_name)

# data transformations
def clean_locations(df):
    return (df
            .with_columns(
                pl.col('Location')
                .custom.to_title_case()
                .str.replace_all('0', 'o', literal=True)
                .str.replace_all('3', 'e', literal=True)
                .str.replace_all('Londen', 'London', literal=True)
                .str.replace_all('Livrepool', 'Liverpool', literal=True)
            ))

def pivot_column(df, index_columns, columns, values):
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    return (df
            .pivot(
                index=index_columns,
                columns=columns,
                values=values,
            )
            .lazy())

def add_revenues_column(df):
    return (df
            .with_columns(
                (pl.col('Price (£) per pack') * pl.col('Quant per Q')).alias('Revenues')
            ))

def add_revenues_and_avg_per_pack(df):
    return (df
            .groupby('Location')
            .agg(
                [
                    pl.sum('Revenues'),
                    pl.mean('Price (£) per pack').round(2)
                ]
            )
            .sort('Location'))

df = (df
      .pipe(clean_locations)
      .pipe(pivot_column, index_columns=['Location', 'Nut Type'], columns='Category', values='Value')
      .pipe(add_revenues_column)
      .pipe(add_revenues_and_avg_per_pack)
      )
    
print(df.collect())
