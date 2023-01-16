import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler,LabelEncoder,OneHotEncoder
from sklearn.model_selection import cross_val_score,StratifiedKFold,RandomizedSearchCV
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import confusion_matrix,precision_score,f1_score,recall_score

def model(dbt, session):
    # dbt configuration
    dbt.config(packages=["pandas","numpy","scikit-learn"])

    # get upstream data
    data = dbt.ref("ml_data_prep").to_pandas()

    # list out covariates we want to use in addition to outcome variable we are modeling - position  
    x = data[['RACE_YEAR','CIRCUIT_NAME','GRID','CONSTRUCTOR_NAME','DRIVER','DRIVERS_AGE_YEARS','DRIVER_CONFIDENCE','CONSTRUCTOR_RELAIBLITY','TOTAL_PIT_STOPS_PER_RACE','ACTIVE_DRIVER','ACTIVE_CONSTRUCTOR', 'POSITION']]
    x = x[(x['ACTIVE_DRIVER']==1)&(x['ACTIVE_CONSTRUCTOR']==1)]

    def position_index(x):
        if x<4:
            return 1
        if x>10:
            return 3
        else :
            return 2

    le = LabelEncoder()
    x['CIRCUIT_NAME'] = le.fit_transform(x['CIRCUIT_NAME'])
    x['CONSTRUCTOR_NAME'] = le.fit_transform(x['CONSTRUCTOR_NAME'])
    x['DRIVER'] = le.fit_transform(x['DRIVER'])
    x['TOTAL_PIT_STOPS_PER_RACE'] = le.fit_transform(x['TOTAL_PIT_STOPS_PER_RACE'])
    
    # we are dropping the columns that we filtered on in addition to our training variable
    X = x.drop(['ACTIVE_DRIVER','ACTIVE_CONSTRUCTOR'],1)
    X['POSITION_LABEL']= X['POSITION'].apply(lambda x: position_index(x))
    X = X.drop(['POSITION'],1)

    encoded_data = X.copy(deep = True)

    return encoded_data