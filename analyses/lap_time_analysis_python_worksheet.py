    # The Snowpark package is required for Python Worksheets. 
    # You can add more packages by selecting them using the Packages control and then importing them.

    import snowflake.snowpark as snowpark
    import pandas as pd 

    def main(session: snowpark.Session): 
        # Your code goes here, inside the "main" handler.
        tableName = 'MRT_LAP_TIMES_YEARS'
        dataframe = session.table(tableName)
        lap_times = dataframe.to_pandas()

        # print table
        print(lap_times)

        # describe the data
        lap_times["LAP_TIME_SECONDS"] = lap_times["LAP_TIME_MILLISECONDS"]/1000
        lap_time_trends = lap_times.groupby(by="RACE_YEAR")["LAP_TIME_SECONDS"].mean().to_frame()
        lap_time_trends.reset_index(inplace=True)
        lap_time_trends["LAP_MOVING_AVG_5_YEARS"] = lap_time_trends["LAP_TIME_SECONDS"].rolling(5).mean()
        lap_time_trends.columns = lap_time_trends.columns.str.upper()

        final_df = session.create_dataframe(lap_time_trends)
        # Return value will appear in the Results tab.
        return final_df