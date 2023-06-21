import polars as pl

FILE_PATH = 'Student Attendance vs Scores.xlsx'

attendance_df = pl.read_excel(FILE_PATH, sheet_name='Attendance Figures')
scores_df = pl.read_excel(FILE_PATH, sheet_name='Student Test Scores', read_csv_options={'skip_rows': 1})

df = (
    attendance_df
    .join(scores_df, on='student_name', how='inner')
    .select(
        pl.col('student_id').alias('Student ID'),
        pl.col('subject').str.replace('Engish', 'English', literal=True).str.replace('Sciece', 'Science', literal=True).alias('Subject'),  # can I inject dictionary/list?
        pl.col('test_score').round().cast(pl.Int8).alias('Test Score'),
        pl.col('student_name').str.split_exact('_', 1).struct.rename_fields(['First Name', 'Last Name']),
        pl.col('attendance_percentage').alias('Attendance Percentage'),
        pl.when(pl.col('attendance_percentage') < 0.7).then('Low Attendance')
          .when(pl.col('attendance_percentage') >= 0.9).then('High Attendance')
          .otherwise('Medium Attendance')
          .alias('Attendance Flag')
    )
    .unnest('student_name')
)

print(df)
