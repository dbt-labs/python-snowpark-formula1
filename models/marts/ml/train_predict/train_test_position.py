import snowflake.snowpark.functions as F
from sklearn.model_selection import train_test_split
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.metrics import confusion_matrix, balanced_accuracy_score
import io
from sklearn.linear_model import LogisticRegression
from joblib import dump, load
import joblib
import logging
import sys
from joblib import dump, load

logger = logging.getLogger("mylog")

def save_file(session, model, path, dest_filename):
  input_stream = io.BytesIO()
  joblib.dump(model, input_stream)
  session._conn.upload_stream(input_stream, path, dest_filename)
  return "successfully created file: " + path

def model(dbt, session):
    dbt.config(
        packages = ['numpy','scikit-learn','pandas','numpy','joblib','cachetools'],
        materialized = "table",
        tags = "train"
    )
    session.sql('create or replace stage MODELSTAGE').collect()
    
    session._use_scoped_temp_objects = False
    version = "1.0"
    logger.info('Model training version: ' + version)

    user_df = dbt.ref("train_test_dataset")

    df_orig=user_df.to_pandas()
    target_col = "POSITION_LABEL"
    split_X = df_orig.drop([target_col], axis=1)
    split_y = df_orig[target_col]

    # Split out train data
    X_train, X_test, y_train, y_test  = train_test_split(split_X, split_y, train_size=0.7, random_state=42)
    train = [X_train, y_train]
    test = [X_test, y_test]
  
    # now we are only training our one model to deploy 
    model = LogisticRegression()
   
    # fit the preprocessing pipeline and the model together  
    model.fit(X_train, y_train)    
    y_pred = model.predict_proba(X_test)[:,1]
    predictions = [round(value) for value in y_pred]
    balanced_accuracy =  balanced_accuracy_score(y_test, predictions)

    # Save the model to a stage
    save_file(session, model, "@MODELSTAGE/driver_position_"+version, "driver_position_"+version+".joblib" )
    logger.info('Model artifact:' + "@MODELSTAGE/driver_position_"+version+".joblib")
    
    #return user_spdf
    snowpark_train_df = session.write_pandas(pd.concat(train, axis=1, join='inner'), "train_table", auto_create_table=True, create_temp_table=True)
    snowpark_test_df = session.write_pandas(pd.concat(test, axis=1, join='inner'), "test_table", auto_create_table=True, create_temp_table=True)
    
    # Add a column indicating train vs test
    return  snowpark_train_df.with_column("DATASET_TYPE", F.lit("train")).union(snowpark_test_df.with_column("DATASET_TYPE", F.lit("test")))