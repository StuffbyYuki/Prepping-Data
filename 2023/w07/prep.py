import polars as pl

acc_holder_df = pl.read_csv('data/Account Holders.csv')
acc_info_df = pl.read_csv('data/Account Information.csv')
tra_detail_df = pl.read_csv('data/Transaction Detail.csv')
tra_path_df = pl.read_csv('data/Transaction Path.csv')

acc_info_df = (
    acc_info_df
    .filter(
        pl.col('Account Holder ID').is_not_null()
    )
    .with_columns(
        pl.col('Account Holder ID').str.split(', ')
    )
    .explode('Account Holder ID')
)

transactions_df = (
    tra_detail_df
    .join(tra_path_df, how='inner', on='Transaction ID')
    .filter(
        (pl.col('Cancelled?')=='N') &
        (pl.col('Value').gt(1000))
    )
)

accounts_df = (
    acc_holder_df
    .with_columns(
        (pl.lit('0') + pl.col('Contact Number').cast(pl.Utf8)).alias('Contact Number')
    )
    .join(
        acc_info_df.with_columns(
            pl.col('Account Holder ID').cast(pl.Int64)
        ), 
        how='inner', 
        on='Account Holder ID'
    )
    .filter(pl.col('Account Type') != ('Platinum'))
)

combined_df = (
    accounts_df
    .join(
        transactions_df, how='inner', left_on='Account Number', right_on='Account_From'
    )
    .rename({'Account_To': 'Account To'})
    .select(
        'Transaction ID',
        'Account To',
        'Transaction Date',
        'Value',
        'Account Number',
        'Account Type',
        'Balance Date',
        'Balance',
        'Name',
        'Date of Birth',
        'Contact Number',
        'First Line of Address'
    )
)
print(combined_df)