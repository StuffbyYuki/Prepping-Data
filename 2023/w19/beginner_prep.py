import polars as pl


# read data
file_name = 'Session Descriptions.csv'
df = pl.scan_csv(file_name)

# define data transformations
def extract_presenter(df):
    result_df = (
        df
        .with_columns(
            pl.col('Description').str.split(':').arr.get(0).str.strip().alias('Presenter'),
            pl.col('Description').str.split(':').arr.get(1).str.strip().alias('Description'),
        )
    )
    return result_df

def add_initials(df):
    result_df = (
        df
        .with_columns(
            (
                pl.col('Presenter')
                .str.split(' ').arr.first().str.slice(0, 1)
                +
                pl.col('Presenter')
                .str.split(' ').arr.last().str.slice(0, 1)
            ).alias('Initials')
        )
    )
    return result_df

def categorize_desc(df):
    result_df = (
        df
        .with_columns(
            pl.when(
                pl.col('Description').str.to_lowercase().str.contains('server', literal=True)
            )
            .then('Server')
            .when(
                pl.col('Description').str.to_lowercase().str.contains('prep', literal=True)
            )
            .then('Prep')
            .when(
                pl.col('Description').str.to_lowercase().str.contains('desktop', literal=True)
            )
            .then('Desktop')
            .when(
                pl.col('Description').str.to_lowercase().str.contains('community', literal=True)
            )
            .then('Community')
            .otherwise('Others')
            .alias('Subject')
        )
    )
    return result_df

def add_deduplication_flg(df):
    result_df = (
        df
        .with_columns(
            pl.when(
                pl.col('Description').str.to_lowercase().str.contains('deduplication', literal=True)
            )
            .then(True)
            .otherwise(False)
            .alias('Deduplication Flag')
        )
    )
    return result_df

def select_columns(df):
    result_df = (
        df
        .filter(
            pl.col('Deduplication Flag')==True
        )
        .select(['Subject', 'Initials', 'Session Number', 'Deduplication Flag'])
    )
    return result_df

# apply functions in sequence
pl.Config(fmt_str_lengths=100)
df = (
    df
    .pipe(extract_presenter)
    .pipe(add_initials)
    .pipe(categorize_desc)
    .pipe(add_deduplication_flg)
    .pipe(select_columns)
)

print(df.collect().head())