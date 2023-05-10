import pandas as pd

def model(dbt, session):
    # dbt configuration
    dbt.config(packages=["pandas"], tags="predict")

    # get upstream data
    encoding = dbt.ref("covariate_encoding").to_pandas()
    
    # variable for year instead of hardcoding it 
    year=2020

    # filter the data based on the specified year
    hold_out_dataset =  encoding.loc[encoding['RACE_YEAR'] == year]
    
    return hold_out_dataset

