import logging
import joblib
import pandas as pd
import os
from snowflake.snowpark import types as T

DB_STAGE = 'MODELSTAGE'
version = '1.0'
# The name of the model file
model_file_path = 'driver_position_'+version
model_file_packaged = 'driver_position_'+version+'.joblib'

# This is a local directory, used for storing the various artifacts locally
LOCAL_TEMP_DIR = f'/tmp/driver_position'
DOWNLOAD_DIR = os.path.join(LOCAL_TEMP_DIR, 'download')
TARGET_MODEL_DIR_PATH = os.path.join(LOCAL_TEMP_DIR, 'ml_model')
TARGET_LIB_PATH = os.path.join(LOCAL_TEMP_DIR, 'lib')

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

def register_udf_for_prediction(p_predictor ,p_session ,p_dbt):

   # The prediction udf

   def predict_position(p_df: T.PandasDataFrame[int, int, int, int,
                                       int, int, int, int, int]) -> T.PandasSeries[int]:
       # Snowpark currently does not set the column name in the input dataframe
       # The default col names are like 0,1,2,... Hence we need to reset the column
       # names to the features that we initially used for training.
       p_df.columns = [*FEATURE_COLS]
      
       # Perform prediction. this returns an array object
       pred_array = p_predictor.predict(p_df)
       # Convert to series
       df_predicted = pd.Series(pred_array)
       return df_predicted

   # The list of packages that will be used by UDF
   udf_packages = p_dbt.config.get('packages')

   predict_position_udf = p_session.udf.register(
       predict_position
       ,name=f'predict_position'
       ,packages = udf_packages
   )
   return predict_position_udf

def download_models_and_libs_from_stage(p_session):
   p_session.file.get(f'@{DB_STAGE}/{model_file_path}/{model_file_packaged}', DOWNLOAD_DIR)
  
def load_model(p_session):
   # Load the model and initialize the predictor
   model_fl_path = os.path.join(DOWNLOAD_DIR, model_file_packaged)
   predictor = joblib.load(model_fl_path)
   return predictor
  
# -------------------------------
def model(dbt, session):
   dbt.config(
       packages = ['snowflake-snowpark-python' ,'scipy','scikit-learn' ,'pandas' ,'numpy'],
       materialized = "table",
       tags = "predict",
       use_anonymous_sproc=True
   )
   session._use_scoped_temp_objects = False
   download_models_and_libs_from_stage(session)
   predictor = load_model(session)
   predict_position_udf = register_udf_for_prediction(predictor, session ,dbt)
  
   # Retrieve the data, and perform the prediction
   hold_out_df = (dbt.ref("hold_out_dataset_for_prediction")
       .select(*FEATURE_COLS)
   )
   trained_model_file = dbt.ref("train_model_to_predict_position")

   # Perform prediction.
   new_predictions_df = hold_out_df.withColumn("position_predicted"
       ,predict_position_udf(*FEATURE_COLS)
   )
  
   return new_predictions_df