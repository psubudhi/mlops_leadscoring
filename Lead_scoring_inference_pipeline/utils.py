"""
File: utils.py
Author: Prem Subudhi
Created: 2024-09-08
Version: 1.0
Description: Utility functions for encoding the feature, getting the prediction from model and checking the features expected.

Functions:
- encode_features, get_models_prediction, prediction_ratio_check, input_features_check 

"""


###############################################################################
# Import necessary modules
# ##############################################################################

import mlflow
import mlflow.sklearn
import pandas as pd

import sqlite3

from Lead_scoring_inference_pipeline.constants import *

import os
import logging

from datetime import datetime

###############################################################################
# Define the function to encode the features
# ##############################################################################
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
        **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline for this.

    OUTPUT
        1. Save the encoded features in a table - features

    SAMPLE USAGE
        encode_features()
    '''
    # read the model input data
    connection = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    df_model_input = pd.read_sql('select * from model_input', connection)

    # create df to hold encoded data and intermediate data
    df_ohef_encoded = pd.DataFrame(columns=ONE_HOT_ENCODED_FEATURES)
    df_placeholder = pd.DataFrame()

    # encode the features using get_dummies()
    for feature in FEATURES_TO_ENCODE:
        if(feature in df_model_input.columns):
            encoded = pd.get_dummies(df_model_input[feature])
            encoded = encoded.add_prefix(feature + '_')
            df_placeholder = pd.concat([df_placeholder, encoded], axis=1)
        else:
            print('Feature not found')
            return df_model_input

    # add the encoded features into a single dataframe
    for feature in df_ohef_encoded.columns:
        if feature in df_model_input.columns:
            df_ohef_encoded[feature] = df_model_input[feature]
        if feature in df_placeholder.columns:
            df_ohef_encoded[feature] = df_placeholder[feature]
    df_ohef_encoded.fillna(0, inplace=True)

    # save the features and target in separate tables
    df_ohef_encoded.to_sql(name='features_inference', con=connection,
                       if_exists='replace', index=False)

    connection.close()


###############################################################################
# Define the function to load the model from mlflow model registry

def get_models_prediction():
    '''
    This function loads the model which is in production from mlflow registry and 
    uses it to do prediction on the input dataset. Please note this function will the load
    the latest version of the model present in the production stage. 

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        model from mlflow model registry
        model name: name of the model to be loaded
        stage: stage from which the model needs to be loaded i.e. production


    OUTPUT
        Store the predicted values along with input data into a table

    SAMPLE USAGE
        load_model()
    '''
    # set the tracking uri
    mlflow.set_tracking_uri(TRACKING_URI)

    # load the latest model from production stage
    loaded_model = mlflow.pyfunc.load_model(
        model_uri=f"models:/{MODEL_NAME}/{STAGE}")

    # read the new data
    connection = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    df_new_data = pd.read_sql('select * from features_inference', connection)

    # run the model to generate the prediction on new data
    y_pred = loaded_model.predict(df_new_data)
    df_new_data['pred_app_complete_flag'] = y_pred

    # store the data in a table
    df_new_data.to_sql(name='predicted_values', con=connection,
                       if_exists='replace', index=False)
    connection.close()


###############################################################################
# Define the function to check the distribution of output column
# ##############################################################################

def prediction_ratio_check():
    '''
    This function calculates the % of 1 and 0 predicted by the model and  
    and writes it to a file named 'prediction_distribution.txt'.This file 
    should be created in the ~/airflow/dags/Lead_scoring_inference_pipeline 
    folder. 
    This helps us to monitor if there is any drift observed in the predictions 
    from our model at an overall level. This would determine our decision on 
    when to retrain our model.
    

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be

    OUTPUT
        Write the output of the monitoring check in prediction_distribution.txt with 
        timestamp.

    SAMPLE USAGE
        prediction_col_check()
    '''

    # get data from predicted_values table
    connection = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    df_predicted_values = pd.read_sql('select * from predicted_values', connection)

    # Get the value_counts of prediction col pred_app_complete_flag.
    value_counts = df_predicted_values['pred_app_complete_flag'].value_counts(normalize=True)

    # Write the df_predicted_values value count with time in a file
    current_time = datetime.now()
    time_format = str(current_time)+' %of 1 = ' + \
        str(value_counts[1])+' %of 2 ='+str(value_counts[0])
    with open(FILE_PATH+'prediction_distribution.txt', 'a') as f:
        f.write(time_format+"\n")

###############################################################################
# Define the function to check input features after encoding 
###############################################################################

def input_features_check():
    '''
    This function checks whether all the input columns are present in our new
    data. This ensures the prediction pipeline doesn't break because of change in
    columns in input data.

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES: List of all the features which need to be present
        in our input data.

    OUTPUT
        It writes the output in a log file based on whether all the columns are present
        or not.
        1. If all the input columns are present then it logs - 'All the models input are present'
        2. Else it logs 'Some of the models inputs are missing'

    SAMPLE USAGE
        input_col_check()
    '''
    # Get the columns from features_inference table 
    connection = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    df_features_inference = pd.read_sql('select * from features_inference', connection)

    # verify if all columns are present as in ONE_HOT_ENCODED_FEATURES
    check = set(df_features_inference.columns) == set(ONE_HOT_ENCODED_FEATURES)

    if check:
        print('All the models input are present')
    else:
        print('Some of the models inputs are missing')