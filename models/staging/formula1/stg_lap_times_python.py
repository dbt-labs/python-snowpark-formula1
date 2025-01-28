from snowflake.snowpark.functions import call_udf

def model(dbt, session):
    # Reference the source Snowpark DataFrame
    df = dbt.ref("stg_lap_times")

    # Add the new column using the registered UDF
    df = df.with_column(
        "official_laptime",
        call_udf("dbt_hwatson.convert_lap_time", df["LAP_TIME_MILLISECONDS"])
    )

    # Return the transformed Snowpark DataFrame
    return df
