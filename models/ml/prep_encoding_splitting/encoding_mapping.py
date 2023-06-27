import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler,LabelEncoder,OneHotEncoder
from sklearn.linear_model import LogisticRegression

def model(dbt, session):
  # dbt configuration
  dbt.config(packages=["pandas","numpy","scikit-learn"])

  # get upstream data
  data = dbt.ref("ml_data_prep").to_pandas()

  # list out covariates we want to use in addition to outcome variable we are modeling - position
  covariates = data[['RACE_YEAR','RACE_NAME','GRID','CONSTRUCTOR_NAME','DRIVER','DRIVERS_AGE_YEARS','DRIVER_CONFIDENCE','CONSTRUCTOR_RELAIBLITY','TOTAL_PIT_STOPS_PER_RACE','ACTIVE_DRIVER','ACTIVE_CONSTRUCTOR', 'DRIVER_POSITION']]
 
  # filter covariates on active drivers and constructors
  # use fil_cov as short for "filtered_covariates"
  fil_cov = covariates[(covariates['ACTIVE_DRIVER']==1)&(covariates['ACTIVE_CONSTRUCTOR']==1)]

  # Encode categorical variables using LabelEncoder
  # TODO: we'll update this to both ohe in the future for non-ordinal variables! 
  le = LabelEncoder()
  fil_cov['DRIVER'] = le.fit_transform(fil_cov['DRIVER'])

  encoding_mapping = pd.DataFrame({'original_driver_value': le.classes_, 'encoded_driver_value': range(len(le.classes_))})
  encoding_mapping.columns = encoding_mapping.columns.str.upper()



  return encoding_mapping


