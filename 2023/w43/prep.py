import polars as pl

attendance_df = (
    pl.read_csv('data/Term*.csv')
    .with_columns(
        (pl.col('First Name') + ' ' + pl.col('Last Name')).alias('Full Name')
    )
)

year_group_df = pl.read_csv('data/Year Groups.csv')

attendance_yearly_df = (
    attendance_df
    .join(
        year_group_df, how='inner', on=['First Name', 'Last Name']
    )
    .group_by('Full Name', 'Year Group')
    .agg(
        pl.col('Days Present').sum(),
        pl.col('Days Absent').sum(),
        (
            pl.col('Days Present').sum() / 
            (pl.col('Days Present').sum() + pl.col('Days Absent').sum() ) *
            100
        ) 
        .round(2) 
        .alias('Year Attendance Rate')
    )
    .with_columns(
        pl.col('Year Attendance Rate').rank('dense', descending=True).alias('Rank')
    )
)

best_attendance_rate = (
    attendance_yearly_df
    .filter(pl.col('Rank')==1)
    .select('Full Name', 'Year Attendance Rate', 'Rank')
)
print(best_attendance_rate)

top_5_perc_list_df =  (
    attendance_yearly_df
    .sort('Rank')
    .group_by('Year Group')
    .agg(
        pl.col('Year Group', 'Year Attendance Rate')
        .head(
            (pl.count() * 0.05)
            .ceil()
        )
    )
)

top_5_attendance = (
    attendance_yearly_df
    .join(top_5_perc_list_df, how='inner', on='Year Group', suffix=' List')
    .filter(
        pl.col('Year Attendance Rate').is_in(pl.col('Year Attendance Rate List'))
    )
    .select('Year Group', 'Full Name', 'Year Attendance Rate')
)
print(top_5_attendance)





