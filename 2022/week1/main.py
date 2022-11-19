import pandas as pd
from config import FILE_URL

# read data
df = pd.read_csv(FILE_URL)

# add a concat name column
df['Pupil Full Name'] = df['pupil last name'] + ', ' + df['pupil first name']
df.drop(columns=['pupil last name', 'pupil first name'], inplace=True)

print(df.head())