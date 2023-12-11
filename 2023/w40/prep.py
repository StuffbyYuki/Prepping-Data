import polars as pl

df = pl.read_csv('data/PD 2023 Wk 40 Input.csv')

class_df = (
    df.melt(
        id_vars='Name', 
        variable_name='Class Type', 
        value_name='Subject', 
        value_vars=df.columns[1:]
    )
    .with_columns(
        # English
        pl.when(
            pl.col('Subject').str.starts_with('En') 
            | pl.col('Subject').str.ends_with('sh')  
        )
        .then(pl.lit('English'))
        # French
        .when(
            pl.col('Subject').str.starts_with('Fren') 
            | pl.col('Subject').str.ends_with('ch')   
        )
        .then(pl.lit('French'))
        # Geography
        .when(
            pl.col('Subject').str.starts_with('Ge') 
            | pl.col('Subject').str.ends_with('phy')   
        )
        .then(pl.lit('Geography'))
        # History
        .when(
            pl.col('Subject').str.starts_with('Hi') 
            | pl.col('Subject').str.ends_with('ry')   
        )
        .then(pl.lit('History'))
        # Math
        .when(
            pl.col('Subject').str.starts_with('M') 
            | pl.col('Subject').str.ends_with('th')   
        )
        .then(pl.lit('Math'))
        # Null
        .when(
            pl.col('Subject') == 'Null'
        )
        .then(pl.lit(None))
        # Physical Education
        .when(
            pl.col('Subject').str.starts_with('Physi') 
            | pl.col('Subject').str.contains('sical') 
            | pl.col('Subject').str.contains('Edu') 
        )
        .then(pl.lit('Physical Education'))
        # Science
        .when(
            pl.col('Subject').str.starts_with('Sc') 
            | pl.col('Subject').str.contains('cie') 
            | pl.col('Subject').str.ends_with('ce') 
        )
        .then(pl.lit('Science'))
        .otherwise(pl.col('Subject')).alias('Subject'),
        # a new column
        pl.when(
            pl.col('Class Type').str.starts_with('Dropped')
        )
        .then(pl.lit('Dropped'))
        .otherwise(pl.lit('Active'))
        .alias('Class Status')
    )
    .filter(pl.col('Subject').is_not_null())
)

aggregated_df = (
    class_df
    .group_by('Subject')
    .agg(
        (pl.col('Class Status')=='Dropped').sum().alias('Dropped'),
        (pl.col('Class Status')=='Active').sum().alias('Active'),
        pl.count().alias('Total Enrolled')
    )
    .with_columns(
        (pl.col('Dropped') / pl.col('Total Enrolled')).round(3).alias('Dropped Rate')
    )
    .sort('Dropped Rate', descending=True)
)
print(aggregated_df)





