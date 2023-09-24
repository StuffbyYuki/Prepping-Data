import polars as pl


def create_lookup_table(dim_lf: pl.LazyFrame, monthly_lf: pl.LazyFrame) -> pl.DataFrame:
    '''
    union dim and monthly data with only employee_id and guid selected
    '''
    dim_id_guid_combinations = (dim_lf.select(pl.col(['employee_id', 'guid'])))
    monthly_id_guid_combinations = (monthly_lf.select(pl.col(['employee_id', 'guid'])))
    lookup_table = (
        pl.concat([dim_id_guid_combinations, monthly_id_guid_combinations], how='vertical')
        .drop_nulls()
        .unique()
        .collect()
    )
    
    return lookup_table

def fill_emp_and_guid(lf: pl.LazyFrame, emp_lookup_dict: dict, guid_lookup_dict: dict) -> pl.LazyFrame:
    ''''
    fill employee_id and guid with a lookup dictionary
    '''
    res_lf = (
        lf
        .with_columns(pl.col('employee_id').fill_null(pl.col('guid').map_dict(guid_lookup_dict)))
        .with_columns(pl.col('guid').fill_null(pl.col('employee_id').map_dict(emp_lookup_dict)))
    )
    return res_lf

def main():

    dim_lf = pl.scan_csv('data/input/ee_dim_input.csv')
    monthly_lf = pl.scan_csv('data/input/ee_monthly_input.csv')
    
    lookup_table = create_lookup_table(dim_lf, monthly_lf)
    emp_lookup_dict = dict(zip(lookup_table.select('employee_id').to_series(), lookup_table.select('guid').to_series()))
    guid_lookup_dict = dict(zip(lookup_table.select('guid').to_series(), lookup_table.select('employee_id').to_series()))

    dim_null_filled_lf = (
        dim_lf
        .pipe(fill_emp_and_guid, emp_lookup_dict, guid_lookup_dict)
    )

    monthly_null_filled_lf = (
        monthly_lf
        .pipe(fill_emp_and_guid, emp_lookup_dict, guid_lookup_dict)
    )

    dim_null_filled_lf.sink_csv('data/output/ee_dim_output.csv')
    monthly_null_filled_lf.sink_csv('data/output/ee_monthly_output.csv')

if __name__ == '__main__':
    main()
