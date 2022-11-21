import pandas as pd
from config import FILE_URL
import numpy as np

# read data
df = pd.read_csv(FILE_URL)

# add columns
# concat name column, parental contact name, parental email address, academic year
df['Pupil Full Name'] = df['pupil last name'] + ', ' + df['pupil first name']

df['Parental Contact Name'] = (
    np.where(df['Parental Contact'] == 1, 
             df['pupil last name'] + ', ' + df['Parental Contact Name_1'],
             df['pupil last name'] + ', ' + df['Parental Contact Name_2'])
)

df['Parental Contact Email Address'] = (
    df['Parental Contact Name'].str.split(', ').str[1]
    + '.'   
    + df['Parental Contact Name'].str.split(', ').str[0]  
    + '@' 
    + df['Preferred Contact Employer'] 
    + '.com'
)

def get_academic_year(date_of_birth_series):
    '''
    base value = 1 when year gt or eq to 2014
    otherwise, we subtract the year from 2014 where the month is > Sep...!
    '''
    date_of_birth_series = df['Date of Birth']
    threshold_year = 2014
    threshold_month = 9
    base_value = 1
    birth_year_series = pd.DatetimeIndex(date_of_birth_series).year
    birth_month_series = pd.DatetimeIndex(date_of_birth_series).month
    gt_or_eq_to_sep = birth_month_series >= threshold_month
    lt_sep = birth_month_series < threshold_month
    gt_or_eq_to_threshold_year = birth_year_series >= threshold_year
    gt_threshold_year = birth_year_series > threshold_year
    lt_threshold_year = birth_year_series < threshold_year
    gt_or_eq_to_sep_and_gt_or_eq_to_threshold_year = np.logical_or(np.logical_and(gt_or_eq_to_threshold_year, gt_or_eq_to_sep),  gt_threshold_year)
    lt_sep_and_lt_threshold_year = np.logical_and(lt_sep, lt_threshold_year)
    gt_or_eq_to_sep_and_lt_threshold_year = np.logical_and(gt_or_eq_to_sep, lt_threshold_year)

    academic_year = (np.where(
        gt_or_eq_to_sep_and_gt_or_eq_to_threshold_year, 
        base_value,
        np.where(
            gt_or_eq_to_sep_and_lt_threshold_year,
            threshold_year - birth_year_series + 1,
            np.where(
                np.logical_or(lt_sep_and_lt_threshold_year, gt_or_eq_to_threshold_year),
                threshold_year - birth_year_series + 2,
                # flag when no logics applied
                -1
            )
        )
    )
    )
    
    return academic_year

df['Academic Year'] = get_academic_year(df['Date of Birth'])


# select only neccesary columns
column_list = ['Academic Year', 'Pupil Full Name', 'Parental Contact Name', 'Parental Contact Email Address']
df = df.loc[:, column_list]

# export the output
df.to_csv('output.csv', index=None)