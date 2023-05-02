# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import pandas as pd 
import numpy as np 

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    tableName = 'PIT_STOPS_JOINED'
    dataframe = session.table(tableName)
    pit_stops_joined = dataframe.to_pandas()

    # print table
    print(pit_stops_joined)

    # provide year so we do not hardcode dates 
    year=2021

    # describe the data
    pit_stops_joined["PIT_STOP_SECONDS"] = pit_stops_joined["PIT_STOP_MILLISECONDS"]/1000
    fastest_pit_stops = pit_stops_joined[(pit_stops_joined["RACE_YEAR"]==year)].groupby(by="CONSTRUCTOR_NAME")["PIT_STOP_SECONDS"].describe().sort_values(by='mean')
    fastest_pit_stops.reset_index(inplace=True)
    fastest_pit_stops.columns = fastest_pit_stops.columns.str.upper()

    final_df = session.create_dataframe(fastest_pit_stops)
    # Return value will appear in the Results tab.
    return final_df