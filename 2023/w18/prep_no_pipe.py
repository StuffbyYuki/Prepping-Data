import polars as pl
from custom_expressions import CustomStringMethodsCollection

# set to show more rows when print
pl.Config.set_tbl_rows(20)

# read data
file_name = 'Messy Nut House Data.csv'
df = pl.scan_csv(file_name)

# data transformations
df = (df
      .with_columns(
                pl.col('Location')
                .custom.to_title_case()
                .str.replace_all('0', 'o', literal=True)
                .str.replace_all('3', 'e', literal=True)
                .str.replace_all('Londen', 'London', literal=True)
                .str.replace_all('Livrepool', 'Liverpool', literal=True)
            ).collect()
      .pivot(
                index=['Location', 'Nut Type'],
                columns='Category',
                values='Value',
            )
            .lazy()
      .with_columns(
                (pl.col('Price (£) per pack') * pl.col('Quant per Q')).alias('Revenues')
            )
      .groupby('Location')
            .agg(
                [
                    pl.sum('Revenues'),
                    pl.mean('Price (£) per pack').round(2)
                ]
            )
            .sort('Location')
      )
    
print(df.collect())
