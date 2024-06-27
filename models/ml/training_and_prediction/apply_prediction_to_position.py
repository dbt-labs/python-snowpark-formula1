import logging
import joblib
import pandas as pd
import os
from snowflake.snowpark import types as T
from snowflake.ml.registry import Registry
from snowflake.snowpark.functions import col


# DB_STAGE = 'MODELSTAGE'
# version = '1.0'
# # The name of the model file


model_name = 'driver_position'

# The feature columns that were used during model training
# and that will be used during prediction
FEATURE_COLS = [
           "RACE_YEAR"
           ,"RACE_NAME"
           ,"GRID"
           ,"CONSTRUCTOR_NAME"
           ,"DRIVER"
           ,"DRIVERS_AGE_YEARS"
           ,"DRIVER_CONFIDENCE"
           ,"CONSTRUCTOR_RELAIBLITY"
           ,"TOTAL_PIT_STOPS_PER_RACE"]

  
# -------------------------------
def model(dbt, session):
   dbt.config(
       packages = ['snowflake-snowpark-python' ,'scipy','scikit-learn' ,'pandas' ,'numpy','snowflake-ml-python'],
       materialized = "table",
       tags = "predict",
       use_anonymous_sproc=True
   )
  
   # Retrieve the data, and perform the prediction
   hold_out_df = (dbt.ref("hold_out_dataset_for_prediction")
       .select(*FEATURE_COLS)
   )

   reg = Registry(session=session, database_name=dbt.this.database, schema_name=dbt.this.schema)
   
   m = reg.get_model(model_name)
   mv = m.default
   new_predictions_df = mv.run(hold_out_df, function_name="predict")
   renamed_df = new_predictions_df.rename(col("output_feature_0"), "position_predicted")

   return renamed_df