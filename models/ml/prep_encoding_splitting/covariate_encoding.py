import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler,LabelEncoder,OneHotEncoder
from sklearn.linear_model import LogisticRegression

def model(dbt, session):
  # dbt configuration
  dbt.config(packages=["pandas==1.5.3","numpy","scikit-learn"])

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
  fil_cov['RACE_NAME'] = le.fit_transform(fil_cov['RACE_NAME'])
  fil_cov['CONSTRUCTOR_NAME'] = le.fit_transform(fil_cov['CONSTRUCTOR_NAME'])
  fil_cov['DRIVER'] = le.fit_transform(fil_cov['DRIVER'])
  fil_cov['TOTAL_PIT_STOPS_PER_RACE'] = le.fit_transform(fil_cov['TOTAL_PIT_STOPS_PER_RACE'])

   # Simply target variable "position" to represent 3 meaningful categories in Formula1
   # 1. Podium position 2. Points for team 3. Nothing - no podium or points!
  def position_index(x):
      if x<4:
          return 1
      if x>10:
          return 3
      else :
          return 2

  # we are dropping the columns that we filtered on in addition to our training variable
  encoded_data = fil_cov.drop(['ACTIVE_DRIVER','ACTIVE_CONSTRUCTOR'],1)
  encoded_data['POSITION_LABEL']= encoded_data['DRIVER_POSITION'].apply(lambda x: position_index(x))
  encoded_data_grouped_target = encoded_data.drop(['DRIVER_POSITION'],1)

  return encoded_data_grouped_target