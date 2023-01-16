import pandas as pd

def model(dbt, session):
    # dbt configuration
    dbt.config(packages=["pandas"])

    # get upstream data
    lap_times = dbt.ref("int_lap_times_years").to_pandas()

    # describe the data
    lap_times["LAP_TIME_SECONDS"] = lap_times["LAP_TIME_MILLISECONDS"]/1000
    lap_time_trends = lap_times.groupby(by="RACE_YEAR")["LAP_TIME_SECONDS"].mean().to_frame()
    lap_time_trends.reset_index(inplace=True)
    lap_time_trends["lap_moving_avg_5_years"] = lap_time_trends["LAP_TIME_SECONDS"].rolling(5).mean()
    
    return lap_time_trends.round(1)