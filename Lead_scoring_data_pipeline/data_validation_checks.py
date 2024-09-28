"""
Import necessary modules
############################################################################## 
"""
import pandas as pd
import sqlite3
from Lead_scoring_data_pipeline.constants import *
from Lead_scoring_data_pipeline.schema import *
###############################################################################
# Define function to validate raw data's schema
############################################################################### 

def raw_data_schema_check():
    '''
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.

   
    INPUTS
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        raw_data_schema : schema of raw data in the form of a list/tuple as present 
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Raw datas schema is in line with the schema present in schema.py' 
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'

    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    leadscoring_raw_data = pd.read_csv(DATA_DIRECTORY+'leadscoring.csv')
    
    isAvailable = set(leadscoring_raw_data.columns) == set(raw_data_schema)
    if isAvailable:
        print('Raw datas schema is in line with the schema present in schema.py')
    else:
        print('Raw datas schema is NOT in line with the schema present in schema.py')
   

###############################################################################
# Define function to validate model's input schema
############################################################################### 

def model_input_schema_check():
    '''
    This function check if all the columns mentioned in model_input_schema in 
    schema.py are present in table named in 'model_input' in db file.

   
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        model_input_schema : schema of models input data in the form oa list/tuple
                          present as in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Models input schema is in line with the schema present in schema.py'
        else prints
        'Models input schema is NOT in line with the schema present in schema.py'
    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    print ("Connecting to database")
    connection = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    print ("Reading data from interactions_mapped table")
    interactions_mapped = pd.read_sql('select * from interactions_mapped', connection)
    interactions_mapped_columns = list(interactions_mapped.columns)
    print("interactions_mapped column length: ", len(interactions_mapped_columns))
    print("model_input_schema length: ", len(model_input_schema))
    
    missmatch = 0
    for column in model_input_schema:
        if column not in interactions_mapped_columns:
            print("column mismatch: ", column)
            missmatch = 1
            
    if missmatch == 0:
        print("Models input schema is in line with the schema present in schema.py")
    else:
        print("Models input schema is NOT in line with the schema present in schema.py")
        
    connection.close()    

#raw_data_schema_check()    
#model_input_schema_check()
