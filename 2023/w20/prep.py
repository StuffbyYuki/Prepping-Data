import polars as pl


# read data
file_name_meal_prices = 'Meal Prices.csv'
file_name_meal_nutritional_info = 'Meal Nutritional Info.csv'
meal_prices_df = pl.scan_csv(file_name_meal_prices)
nutritions_df = pl.scan_csv(file_name_meal_nutritional_info)

# data transformations
def consolidate_mean_type(df):
    result_df = (
        df
        .with_columns(
            pl.col('Type')
            .str.replace('Veggie', 'Vegetarian', literal=True)
            .str.replace('Meat based', 'Meat-based', literal=True)
        )
    )

    return result_df

def join_dfs(df1, df2):
    result_df = (
        df1.join(df2, how='left', on='Meal Option')
    )
    return result_df

def add_calculations_per_type(df):
    result_df = (
        df
        .groupby('Type')
        .agg(
            pl.mean('Price').round(2).alias('Average Price'),
            pl.count().alias('cnt')
        )   
        .with_columns(
            ((pl.col('cnt') / pl.col('cnt').sum()).round(2) * 100).cast(pl.UInt64).alias('Percent of Total Cnt')
        )
    )
    return result_df 

def select_and_sort(df):
    result_df = (
        df
        .drop('cnt')
        .sort('Type')
    )   

    return result_df

# result
df = (
    nutritions_df
    .pipe(consolidate_mean_type)
    .pipe(join_dfs, meal_prices_df)
    .pipe(add_calculations_per_type)
    .pipe(select_and_sort)
)

print(df.collect().head())
