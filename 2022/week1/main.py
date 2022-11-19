import os

#USE ONLY ONE OF THESE:

os.environ["MODIN_ENGINE"] = "ray"  # Modin will use Ray
os.environ["MODIN_ENGINE"] = "dask"  # Modin will use Dask

import modin.pandas as pd

df = pd.read_csv('https://drive.google.com/file/d/1p8gt3cR3ATCeGK81pnT90x0a6dbCXst1/view')

print(df.head())