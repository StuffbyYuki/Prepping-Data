import polars as pl

acc_info_df = pl.read_csv('data/Account Information.csv')
tra_detail_df = pl.read_csv('data/Transaction Detail.csv')
tra_path_df = pl.read_csv('data/Transaction Path.csv')

transactions_df = (
    tra_detail_df
    .join(
        tra_path_df, how='inner', on='Transaction ID'
    )
    .filter(pl.col('Cancelled?') == 'N')
)

all_transactions_per_account = (
    pl.concat([
        (
            acc_info_df
            .join(
                transactions_df.with_columns(pl.lit('outgoing').alias('Transaction Type')),
                left_on='Account Number',
                right_on='Account_From',
                how='left',
            )
            .select(pl.exclude('Account_From', 'Account_To'))
        ),
        (
            acc_info_df
            .join(
                transactions_df.with_columns(pl.lit('incoming').alias('Transaction Type')),
                left_on='Account Number',
                right_on='Account_To',
                how='left',
            )
            .select(pl.exclude('Account_From', 'Account_To'))
        )
    ])
    .rename({'Balance': 'Initial Balance', 'Value': 'Transaction Value'})
)


account_summary = (
    all_transactions_per_account
    .select(
        'Account Number',
        'Initial Balance',
        'Transaction Date',
        'Transaction Value',
        'Transaction Type',
    )
    .with_columns(
        pl.when(pl.col('Transaction Type') == 'incoming')
        .then(pl.col('Transaction Value'))
        .when(pl.col('Transaction Type') == 'outgoing')
        .then(pl.col('Transaction Value') * -1)
        .alias('Transaction Value')
    )
    .sort(
        'Account Number',
        'Transaction Date',
        'Transaction Value',
        descending=[False, False, True],
    )
    .select(
        'Account Number',
        pl.col('Transaction Date').alias('Balance Date'),
        'Transaction Value',
        (pl.col('Initial Balance') + pl.col('Transaction Value').cum_sum())
        .over('Account Number')
        .alias('Balance'),
    )
    .filter(
        ~pl.all_horizontal(
            pl.col('Balance Date').is_null(),
            pl.col('Transaction Value').is_null(),
            pl.col('Balance').is_null()
        )
    )
)

account_initial_balance = (
    all_transactions_per_account.select(
        'Account Number', 'Balance Date', 'Initial Balance'
    )
    .unique()
    .with_columns(pl.lit(None).alias('Transaction Value'))
    .rename({'Initial Balance': 'Balance'})
    .select('Account Number', 'Balance Date', 'Transaction Value', 'Balance')
)

output = (
    pl.concat([
        account_summary, 
        account_initial_balance
    ])
    .sort('Account Number', 'Balance Date', 'Transaction Value')
    .with_columns(pl.col('Balance').round(2))
)

output.write_csv('data/Output.csv')
