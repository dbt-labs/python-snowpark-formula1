import logging
import joblib
import pandas as pd
import os
from snowflake.snowpark import types as T
from snowflake.ml.registry import Registry

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
   session._use_scoped_temp_objects = False
   reg = Registry(session=session, database_name="DEV_TROUZEDB", schema_name="DBT_TROUZE_ML")
   m = reg.get_model("DRIVER_POSITION")
   mv = m.default

   # Retrieve the data, and perform the prediction
   hold_out_df = (dbt.ref("hold_out_dataset_for_prediction")
       .select(*FEATURE_COLS)
   )

   # Perform prediction.
   new_predictions_df = mv.run(hold_out_df, function_name="predict")

   return new_predictions_df