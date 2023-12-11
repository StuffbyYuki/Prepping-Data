import polars as pl

names_df = pl.read_csv('data/Student_Name.csv')
nationality_df = pl.read_csv('data/Student_Nationality.csv')

df = (
    nationality_df
    .join(names_df, on='Student ID', how='left')
    .group_by('Nationality', 'Classroom')
    .agg(
        pl.count().alias('Student Cnt')
    )
    .with_columns(
        pl.col('Student Cnt').rank('ordinal', descending=True).over('Classroom').alias('Rank')
    )
    .filter(pl.col('Rank')==1)
    .select(pl.exclude('Rank'))
)
print(df)
