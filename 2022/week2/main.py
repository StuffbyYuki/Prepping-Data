import pandas as pd
import numpy as np
from datetime import datetime

# read data
file_url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRTsH5Ux6FjZREk84LMVgYR61b2NPHN6-2eQH8qUlDaI4mvCnPKVsqth99wcdW-JQl35sBw2KYRWUba/pub?output=csv'
df = pd.read_csv(file_url)

# drop unneccesary columns

df = df.drop(columns=['Parental Contact Name_1', 'Parental Contact Name_2', 'Preferred Contact Employer', 'Parental Contact', 'id', 'gender'])


# add columns
# pupil name
df['Pupil Full Name'] = df['pupil first name'] + ' ' + df['pupil last name']

# birth day in curr year
current_year = datetime.now().year
df['This Year\'s Birthday'] = (    
    pd.to_datetime(df['Date of Birth']).dt.to_period('D').dt.to_timestamp() + pd.DateOffset(year=current_year)
)

print(df.head())

# birthday month
df['Month'] = df['This Year\'s Birthday'].dt.month_name()

# day cake needed on
birthday_day_name_s = df['This Year\'s Birthday'].dt.day_name()
df['Cake Needed On'] = np.where(
    birthday_day_name_s.isin(['Saturday', 'Sunday']),
    'Friday',
    birthday_day_name_s
)
   
# add count per weekday per month
grouped_by_columns = ['Cake Needed On', 'Month']
bd_per_weekday_month_df = df.groupby(grouped_by_columns).size().reset_index(name='Birthdays per Weekday and Month') 
df = df.merge(bd_per_weekday_month_df, on=grouped_by_columns, how='inner')


# select only necessary columns
columns_needed = ['Pupil Full Name', 'Date of Birth', 'This Year\'s Birthday', 'Month', 'Cake Needed On', 'Birthdays per Weekday and Month']
df = df.loc[:, columns_needed].sort_values(by='Pupil Full Name')

# export the output
# df.to_csv('output.csv', index=None)