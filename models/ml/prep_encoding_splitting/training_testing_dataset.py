import pandas as pd

def model(dbt, session):

    # dbt configuration
    dbt.config(packages=["pandas"], tags="train")

    # get upstream data
    encoding = dbt.ref("covariate_encoding").to_pandas()

    # provide years so we do not hardcode dates in filter command
    start_year=2010
    end_year=2019

    # describe the data for a full decade
    train_test_dataset =  encoding.loc[encoding['RACE_YEAR'].between(start_year, end_year)]

    return train_test_dataset