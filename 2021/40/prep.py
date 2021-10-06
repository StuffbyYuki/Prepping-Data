# Prepping data week 40 
# source: https://preppindata.blogspot.com/2021/10/2021-week-40-animal-adoptions.html
# Created by: Yuki Kakegawa - https://github.com/StuffbyYuki

import pandas as pd
import numpy as np
import plotly.express as px

# read in the file
PATH = './2021/40/'
FILE = 'Austin_Animal_Center_Outcomes.csv'
df = pd.read_csv(PATH + FILE)

# remove duplicates on date
df = df.drop_duplicates('DateTime')

# only keep cats and dogs
allowed_animals = ['Cat', 'Dog']
df = df[df['Animal Type'].isin(allowed_animals)]

# group up outcome types into 2 groups
target_outcome_types = ['Rto-Adopt', 'Adoption', 'Transfer', 'Return to Owner']
df['Adopted, Returned to Owner or Transferred'] = np.where(df['Outcome Type'].isin(target_outcome_types), 'Adopted, Returned to Owner or Transferred', 'Other')

# get percentage of each group by animal type
output = ( pd.crosstab(index=df['Animal Type'], columns=df['Adopted, Returned to Owner or Transferred'], normalize='index') 
             .apply(lambda x: x*100) 
             .round(1) 
             .sort_values(by='Animal Type', ascending=False) )

# export the result and visualize it
output.to_csv(PATH + 'output.csv')

fig = px.bar(output, x=output.index, y=['Adopted, Returned to Owner or Transferred', 'Other'], title="Prepping data 2021 - week 40")
fig.update_layout(legend_title_text='Type')
fig.show()