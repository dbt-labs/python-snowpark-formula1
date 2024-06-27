import snowflake.snowpark.functions as F
from sklearn.model_selection import train_test_split
import pandas as pd
from sklearn.metrics import confusion_matrix, balanced_accuracy_score
import io
from sklearn.linear_model import LogisticRegression
from joblib import dump, load
import joblib
import logging
import sys
from joblib import dump, load
from snowflake.ml.registry import Registry

logger = logging.getLogger("mylog")

#credit: code borrowed from https://medium.com/snowflake/getting-started-with-snowpark-model-registry-131e5a2783c4
def get_next_version(reg, model_name) -> str:
    models = reg.show_models()
    if models.empty:
        return "V_1"
    elif model_name not in models["name"].to_list():
        return "V_1"
    max_version = max(
        ast.literal_eval(models.loc[models["name"] == model_name, "versions"].values[0])
    )
    return f"V_{int(max_version.split('_')[-1]) + 1}"

def register_model(session, reg, model_name, model, sample_input_data):
    reg.log_model(
    model_name=model_name,
    version_name=get_next_version(reg, model_name),
    model=model, 
    sample_input_data = sample_input_data
    )
    return "successfully registered model: " + model_name

def model(dbt, session):
   dbt.config(
       packages = ['numpy','scikit-learn','pandas','numpy','joblib','cachetools','snowflake-ml-python'],
       materialized = "table",
       tags = "train"
   )
   # Create a stage in Snowflake to save our model file
   #session.sql('create or replace stage MODELSTAGE').collect()
  
   #session._use_scoped_temp_objects = False
   version = "1.0"
   logger.info('Model training version: ' + version)

   reg = Registry(session=session, database_name=dbt.this.database, schema_name=dbt.this.schema)


   # read in our training and testing upstream dataset
   test_train_df = dbt.ref("training_testing_dataset")

   #  cast snowpark df to pandas df
   test_train_pd_df = test_train_df.to_pandas()
   target_col = "POSITION_LABEL"

   # split out covariate predictors, x, from our target column position_label, y.
   split_X = test_train_pd_df.drop([target_col], axis=1)
   split_y = test_train_pd_df[target_col]

   # Split out our training and test data into proportions
   X_train, X_test, y_train, y_test  = train_test_split(split_X, split_y, train_size=0.7, random_state=42)
   train = [X_train, y_train]
   test = [X_test, y_test]
    # now we are only training our one model to deploy
   # we are keeping the focus on the workflows and not algorithms for this lab!
   model = LogisticRegression()
 
   # fit the preprocessing pipeline and the model together 
   model.fit(X_train, y_train)   
   y_pred = model.predict_proba(X_test)[:,1]
   predictions = [round(value) for value in y_pred]
   balanced_accuracy =  balanced_accuracy_score(y_test, predictions)


   # Take our pandas training and testing dataframes and put them back into snowpark dataframes
   snowpark_train_df = session.write_pandas(pd.concat(train, axis=1, join='inner'), "train_table", auto_create_table=True, create_temp_table=True)
   snowpark_test_df = session.write_pandas(pd.concat(test, axis=1, join='inner'), "test_table", auto_create_table=True, create_temp_table=True)

    # Save the model to a stage
    #save_file(session, model, "@MODELSTAGE/driver_position_"+version, "driver_position_"+version+".joblib" )
   model_name = 'driver_position'
   register_model(session, reg, model_name, model, sample_input_data =split_X )
   logger.info('Model name: ' + model_name)
  
   # Union our training and testing data together and add a column indicating train vs test rows
   return  snowpark_train_df.with_column("DATASET_TYPE", F.lit("train")).union(snowpark_test_df.with_column("DATASET_TYPE", F.lit("test")))