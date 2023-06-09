import polars as pl

file_name = 'school_grades.csv'

def unpivot_grades(df):
    return (
        df
        .melt(
            id_vars=['student_id','first_name','last_name','Gender','D.O.B'],
            value_vars=['2021-attainment', '2021-effort', '2021-attendance', '2021-behaviour', '2022-attainment', '2022-effort', '2022-attendance', '2022-behaviour'],
            variable_name='grade_type',
            value_name='grade'
        )
    )

def add_grade_columns(df):
    return (
        df
        .with_columns(
            pl.col('grade').filter(pl.col('grade_type').str.contains('2021', literal=True)).mean().over('student_id').alias('grade_2021'),
            pl.col('grade').filter(pl.col('grade_type').str.contains('2022', literal=True)).mean().over('student_id').alias('grade_2022')
        )
    )

def drop_unneccary_columns(df):
    return (
        df
        .drop('grade', 'grade_type')
        .unique()
    )

def add_grade_diff(df):
    return (
        df
        .with_columns(
            (pl.col('grade_2022') - pl.col('grade_2021')).alias('diff')
        )
    )

def keep_only_under_perf_students(df):
    return (
        df
        .filter(pl.col('diff') < 0)
        .sort('student_id')
    )

lazy_df = pl.scan_csv(file_name)
print(lazy_df.columns)
print(lazy_df.fetch())

result_df = (
    lazy_df
    .pipe(unpivot_grades)
    .pipe(add_grade_columns)
    .pipe(drop_unneccary_columns)
    .pipe(add_grade_diff)
    .pipe(keep_only_under_perf_students)
).collect()

print(result_df)
