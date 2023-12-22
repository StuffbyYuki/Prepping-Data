import polars as pl

df = pl.read_csv('data/DSB Customer Survey.csv')

mobile_cols = [col for col in df.columns if 'Mobile' in col]
online_cols = [col for col in df.columns if 'Online' in col]

mobile = (
    df.melt(
        id_vars='Customer ID', 
        value_vars=online_cols, 
        variable_name='Functionality', 
        value_name='Online Score'
    )
    .with_columns(
        pl.col('Functionality').str.split(' - ').list.get(1),
        pl.lit('Online').alias('Platform')
    )
)

online = (
    df.melt(
        id_vars='Customer ID', 
        value_vars=mobile_cols, 
        variable_name='Functionality', 
        value_name='Mobile Score'
    )
    .with_columns(
        pl.col('Functionality').str.split(' - ').list.get(1),
        pl.lit('Mobile').alias('Platform')
    )
)

cleaned = (
    mobile
    .join(online, how='inner', on=['Customer ID', 'Functionality'])
    .filter(
        pl.col('Functionality').ne('Overall Rating')
    )
    .with_columns(
        pl.col('Mobile Score').mean().over('Customer ID', 'Platform').alias('Mobile Avg Rating'),
        pl.col('Online Score').mean().over('Customer ID', 'Platform').alias('Online Avg Rating')
    )
    .with_columns(
        (pl.col('Online Avg Rating') - pl.col('Mobile Avg Rating')).alias('Rating Diff')
    )
    .with_columns(
        pl.when(pl.col('Rating Diff').le(-2)).then(pl.lit('Mobile App Superfan'))
        .when(pl.col('Rating Diff').le(-1)).then(pl.lit('Mobile App Fan'))
        .when(pl.col('Rating Diff').ge(2)).then(pl.lit('Online Interface Superfan'))
        .when(pl.col('Rating Diff').ge(1)).then(pl.lit('Online Interface Fan'))
        .when(pl.col('Rating Diff').is_between(-1, 1)).then(pl.lit('Neutral'))
        .alias('Preference')
    )
    .group_by('Preference')
    .agg(pl.count())
    .select(
        'Preference',
        (pl.col('count') / pl.sum('count') * 100).round(1).alias('% of Total')
    )
)
print(cleaned.head())
