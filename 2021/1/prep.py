# Prepping data 2021 week1: https://preppindata.blogspot.com/2021/01/2021-week-1.html
# To run program: python 2021/1/prep.py
# Author: Yuki Kakegawa

from pathlib import Path
import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 20)

# read in the data
BASE_DIR = Path(__file__).resolve().parent#.parent
INPUT_FILE_PATH = BASE_DIR.joinpath('PD 2021 Wk 1 Input.xlsx')
df = pd.read_excel(INPUT_FILE_PATH)

# helper functions
def str_before_hyphen(string):
    return string.split('-')[0].strip()

def str_after_hyphen(string):
    return string.split('-')[1].strip()

# transoformations
df['Store'] = df['Store - Bike'].apply(str_before_hyphen)
df['Bike'] = df['Store - Bike'].apply(str_after_hyphen)
df['Bike'] = df['Bike'].replace({ 'Mountaen' : 'Mountain', 'Rowd': 'Road', 'Rood' : 'Road', 'Graval' : 'Gravel' , 'Gravle' : 'Gravel' })  
df.drop(columns=['Store - Bike'], inplace=True)

df['Quarter'] = df['Date'].dt.quarter 
df['Day of Month'] = df['Date'].dt.day 
df.drop(columns=['Date'], inplace=True)
df = df.iloc[10:,:]

#output to a csv
OUTPUT_FILE_PATH = BASE_DIR.joinpath('output.csv')
df.to_csv(OUTPUT_FILE_PATH, index=None)
print('Output file saved to', OUTPUT_FILE_PATH)

