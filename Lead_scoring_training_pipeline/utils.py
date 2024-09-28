"""
File: utils.py
Author: Prem Subudhi
Created: 2024-09-08
Version: 1.0
Description: Utility functions for lead scoring training .

"""


###############################################################################
# Import necessary modules
# ##############################################################################

import contextlib
import logging
import pandas as pd
import sqlite3
from sqlite3 import Error
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import lightgbm as lgb
from sklearn.metrics import *
from datetime import date
from Lead_scoring_training_pipeline.constants import *
import yaml
import joblib
import os
from pycaret.classification import *
###############################################################################
# Define the function to encode features
# ##############################################################################

def encode_features():
    '''
    This function one hot encodes the categorical features present in our  
    training dataset. This encoding is needed for feeding categorical data 
    to many scikit-learn models.

    INPUTS
        db_file_name : Name of the database file 
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES : list of the features that needs to be there in the final encoded dataframe
        FEATURES_TO_ENCODE: list of features  from cleaned data that need to be one-hot encoded
       .

    OUTPUT
        1. Save the encoded features in a table - features
        2. Save the target variable in a separate table - target


    SAMPLE USAGE
        encode_features()
        
     **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline for this
    '''
    # read the model input data
    cnx = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    df_model_input = pd.read_sql('select * from model_input', cnx)

    # create df to hold encoded data and intermediate data
    df_encoded = pd.DataFrame(columns=ONE_HOT_ENCODED_FEATURES)
    df_placeholder = pd.DataFrame()

    # encode the features using get_dummies()
    for f in FEATURES_TO_ENCODE:
        if(f in df_model_input.columns):
            encoded = pd.get_dummies(df_model_input[f])
            encoded = encoded.add_prefix(f + '_')
            df_placeholder = pd.concat([df_placeholder, encoded], axis=1)
        else:
            print('Feature not found')
            return df_model_input

    # add the encoded features into a single dataframe
    for feature in df_encoded.columns:
        if feature in df_model_input.columns:
            df_encoded[feature] = df_model_input[feature]
        if feature in df_placeholder.columns:
            df_encoded[feature] = df_placeholder[feature]
    df_encoded.fillna(0, inplace=True)

    # save the features and target in separate tables
    df_features = df_encoded.drop(['app_complete_flag'], axis=1)
    df_target = df_encoded[['app_complete_flag']]
    df_features.to_sql(name='features', con=cnx,
                       if_exists='replace', index=False)
    df_target.to_sql(name='target', con=cnx, if_exists='replace', index=False)

    cnx.close()


###############################################################################
# Define the function to train the model
# ##############################################################################

def get_trained_model():
    '''
    This function setups mlflow experiment to track the run of the training pipeline. It 
    also trains the model based on the features created in the previous function and 
    logs the train model into mlflow model registry for prediction. The input dataset is split
    into train and test data and the auc score calculated on the test data and
    recorded as a metric in mlflow run.   

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be


    OUTPUT
        Tracks the run in experiment named 'Lead_Scoring_Training_Pipeline'
        Logs the trained model into mlflow model registry with name 'LightGBM'
        Logs the metrics and parameters into mlflow run
        Calculate auc from the test data and log into mlflow run  

    SAMPLE USAGE
        get_trained_model()
    '''

    _create_sqlit_connection(MLFLOW_DB_PATH, DB_FILE_MLFLOW)

    cnx = sqlite3.connect(DB_PATH + DB_FILE_NAME)
    if (not _is_table_already_populated(cnx, "features")
            and not _is_table_already_populated(cnx, "target")):
        print("features or target table does not exist")
        return

    print("Loading 'features' table")
    X = pd.read_sql("select * from features", cnx)

    print("Loading 'target' table")
    y = pd.read_sql("select * from target", cnx)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=100
    )

    # Model Training

    # make sure to run mlflow server before this.

    run_name = EXPERIMENT + "" + date.today().strftime("%d%m_%Y_%H_%M_%S")
    mlflow.set_tracking_uri(TRACKING_URI)

    with contextlib.suppress(Exception):
        # Creating an experiment
        logging.info("Creating mlflow experiment")
        mlflow.create_experiment(EXPERIMENT)
    # Setting the environment with the created experiment
    mlflow.set_experiment(EXPERIMENT)

    with mlflow.start_run(run_name=run_name) as run:
        # Model Training
        clf = lgb.LGBMClassifier()
        clf.set_params(**model_config)
        clf.fit(X_train, y_train)

        mlflow.sklearn.log_model(
            sk_model=clf, artifact_path="models", registered_model_name="LightGBM"
        )
        
        mlflow.log_params(model_config)

        # predict the results on training dataset
        y_pred = clf.predict(X_test)

        # Log metrics
        acc = accuracy_score(y_pred, y_test)
        precision = precision_score(y_pred, y_test, average="macro")
        recall = recall_score(y_pred, y_test, average="macro")
        f1 = f1_score(y_pred, y_test, average="macro")
        auc = roc_auc_score(y_pred, y_test, average="weighted", multi_class="ovr")
        cm = confusion_matrix(y_test, y_pred)
        tn = cm[0][0]
        fn = cm[1][0]
        tp = cm[1][1]
        fp = cm[0][1]

        print("Precision=", precision)
        print("Recall=", recall)
        print("AUC=", auc)

        mlflow.log_metric("test_accuracy", acc)
        mlflow.log_metric("Precision", precision)
        mlflow.log_metric("Recall", recall)
        mlflow.log_metric("f1", f1)
        mlflow.log_metric("AUC", auc)
        mlflow.log_metric("True Negative", tn)
        mlflow.log_metric("False Negative", fn)
        mlflow.log_metric("True Positive", tp)
        mlflow.log_metric("False Positive", fp)

        run_id = run.info.run_uuid
        print(f"Inside MLflow Run with id {run_id}")
        #save_model(clf, 'best_model')
                model_pkl_file = "model.pkl"  

        #with open(model_pkl_file, 'wb') as file:  
        #    pickle.dump(clf, file)
        #mlflow.log_artifact(clf, "model")
       
        run_id = run.info.run_uuid
        
        config = {
            'model_params': model_config,
        }
        #MLRUN_PATH = "/home/airflow/dags/mlruns/"
        #model_filename = os.path.join(MLRUN_PATH, "model_config.yaml)
        with open("/home/airflow/dags/mlruns/model_config.file", 'w') as file:
            yaml.dump(config, file)
        with open("/home/airflow/dags/mlruns/model_config.yaml", 'w') as file:
            yaml.dump(config, file)
            #save_model_config(clf, model_config, {"test_accuracy": acc}, MLRUN_PATH)
        mlflow.log_artifact("/home/airflow/dags/mlruns/model_config.file")
        mlflow.log_artifact("/home/airflow/dags/mlruns/model_config.yaml")
        
        print(f"Inside MLflow Run with id {run_id}")
        
        
def _is_table_already_populated(cnx, table_name):
    """
    Description: Checks whether the table in the database connection has any values
    input: database connection and table name
    output: boolean
    """
    # cnx = sqlite3.connect(db_path+db_file_name)
    check_table = pd.read_sql(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';",
        cnx,
    ).shape[0]
    return check_table == 1


def _create_sqlit_connection(db_path, db_file):
    """
    Description: create a database connection to a SQLite database
    input: database path and database file
    output:None
    """
    conn = None
    # opening the connection for creating the sqlite db
    try:
        conn = sqlite3.connect(db_path + db_file)
        print(sqlite3.version)
    # return an error if connection not established
    except Error as e:
        print(e)
    # closing the connection once the database is created
    finally:
        if conn:
            conn.close()
      

def save_model_config(model, params, error, output_path):
    # Create a configuration dictionary
    config = {
        'model_params': params,
        'error_metrics': error
    }
    
    # Save configuration to a YAML file
    with open(os.path.join(output_path, 'model_config.yaml'), 'w') as file:
        yaml.dump(config, file)
    
    # Save the model as a .pkl file
    model_filename = os.path.join(output_path, "model.pkl")
    #joblib.dump(model, model_filename)
    try: 
        joblib.dump(model, model_filename) 
    except Exception as e: 
        logging.info(f"Failed to save model: {str(e)}")
    