##############################################################################
# Import necessary modules and files
##############################################################################


import pandas as pd
import os
import sqlite3
from sqlite3 import Error
import numpy as np
from ydata_profiling import ProfileReport
#from pandas_profiling import ProfileReport
import constants
from Maps.city_tier import city_tier_mapping

import matplotlib.pyplot as plt
import mlflow
from pycaret.classification import *
import pandas as pd
from sqlalchemy import create_engine
from constants import *
from significant_categorical_level import *
###############################################################################
# Define the function to build database
###############################################################################



def read_csv_file(file_path):
    """
    Reads a CSV file and returns a DataFrame.

    Parameters:
    file_path (str): The path to the CSV file.

    Returns:
    pd.DataFrame: The DataFrame containing the CSV data.
    """
    try:
        df = pd.read_csv(file_path, index_col=[0])
        return df
    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
    except pd.errors.EmptyDataError:
        print("Error: The file is empty.")
    except pd.errors.ParserError:
        print("Error: Error parsing the file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    # Example usage
    # df = read_csv_file('path/to/your/file.csv')
    # print(df.head())

def write_profile_report(df,file_path,report_path):
    """
    Reads a Dataframe form CSV file.

    Parameters:
    file_path (str): The path to the CSV file.

    Write the proffile report to raw_data_report.html.
    """
    #df_lead_scoring = read_csv_file(CSV_FILE_PATH+CSV_FILE_NAME)
    df_lead_scoring = read_csv_file(report_path+file_path)
    profile = ProfileReport(df, title="Raw Data Summary")
    profile.to_notebook_iframe()
    profile.to_file(report_path)


def read_and_write_to_sqlite(input_df, table_name='output_table'):
    """
    Reads input data and writes the output to the SQLite database 'utils_output.db'.

    Args:
    - input_df (pd.DataFrame): The input DataFrame to be written to the database.
    - table_name (str): The name of the table in the SQLite database where data will be written. Default is 'output_table'.

    Returns:
    None
    """

    # Create a connection to the SQLite database (or create it if it doesn't exist)
    engine = create_engine('sqlite:///utils_output.db')

    # Write the DataFrame to the specified table in the SQLite database
    input_df.to_sql(table_name, engine, if_exists='replace', index=False)

    # Close the connection
    engine.dispose()
    print(f"Data written to the table '{table_name}' in 'utils_output.db'.")


def build_dbs():
    '''
    This function checks if the db file with specified name is present 
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates 
    the db file with the given name at the given path. 


    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should exist  


    OUTPUT
    The function returns the following under the given conditions:
        1. If the file exists at the specified path
                prints 'DB Already Exists' and returns 'DB Exists'

        2. If the db file is not present at the specified loction
                prints 'Creating Database' and creates the sqlite db 
                file at the specified path with the specified name and 
                once the db file is created prints 'New DB Created' and 
                returns 'DB created'
    SAMPLE USAGE
        build_dbs()
    '''
    conn = None
    # Full path of the database file
    db_full_path = os.path.join(DB_PATH, DB_FILE_NAME)
    
        # Check if the database file already exists
    if os.path.isfile(db_full_path):
        print(f"Database '{DB_FILE_NAME}' already exists in '{DB_PATH}'.")
        return 'DB Exists'
    else:
        print(f"Database '{DB_FILE_NAME}' does not exist. Creating new database in '{DB_PATH}'.")
        # If the file does not exist, create it
        try:
            # Establish a connection to create the database
            conn = sqlite3.connect(db_full_path)
            return 'DB created'
        except Error as e:
            print(f"Error creating database: {e}")
            return None
        finally:
            if conn:
                conn.close()
                
###############################################################################
# Define function to load the csv file to the database
###############################################################################

def load_data_into_db():
    '''
    Thie function loads the data present in data directory into the db
    which was created previously.
    It also replaces any null values present in 'toal_leads_dropped' and
    'referred_lead' columns with 0.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        

    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function 
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    '''
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)

        #if _is_table_already_populated(conn, "loaded_data"):
        #    print("loaded_data already populated")
        #    return
        print("Load CSV file: " + CSV_FILE_PATH+LEADSCORING_TEST_CSV_PATH)
        df = read_csv_file(CSV_FILE_PATH+LEADSCORING_TEST_CSV_PATH)
        print("load_data_into_db Head=",df.head())
        print("Process columns for NAN to 0 for total_leads_dropped and referred_lead columns")
        df["total_leads_droppped"] = df["total_leads_droppped"].fillna(0)
        df["referred_lead"] = df["referred_lead"].fillna(0)
        print(df.columns)
        print("DF Index")
        print(df.index)
        print("Saves the processed dataframe in the db table loaded_data")

        if 'created_date' not in df.columns: 
            print("Adding 'created_date' column") 
            df['created_date'] = df.index # Create from index if applicable 
        else: 
            print("'created_date' column already exists")
        df.to_sql(name="loaded_data", con=conn, if_exists="replace", index=False)
        
    except Exception as e:
        print(f"Exception thrown in load_data_into_db : {e}")
    finally:
        if conn:
            conn.close()
###############################################################################
# Define function to map cities to their respective tiers
###############################################################################

    
def map_city_tier():
    '''
    This function maps all the cities to their respective tier as per the
    mappings provided in the city_tier_mapping.py file. If a
    particular city's tier isn't mapped(present) in the city_tier_mapping.py 
    file then the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier

    
    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_city_tier()

    '''
    loaded_data_conn = None
    try:
        loaded_data_conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        #if _is_table_already_populated(conn, "city_tier_mapped"):
        #    print("city_tier_mapped already populated")
        #    return

        print("Loading loaded_data table")
        df_lead_scoring = pd.read_sql("select * from loaded_data", loaded_data_conn)

        print("Mapping city_mapped to tiers")
        df_lead_scoring["city_tier"] = df_lead_scoring["city_mapped"].map(city_tier_mapping)
        df_lead_scoring["city_tier"] = df_lead_scoring["city_tier"].fillna(3.0)
        #print("df_lead_scoring  COLUMNS are: ",df_lead_scoring.columns)
        # we do not need city_mapped later
        df_lead_scoring = df_lead_scoring.drop(["city_mapped"], axis=1)

        print("Saves the processed dataframe in the db in a table named'city_tier_mapped'.")
        df_lead_scoring.to_sql(name="city_tier_mapped", con=loaded_data_conn, if_exists="replace", index=False)

    except Exception as e:
        print(f"Exception thrown in map_city_tier : {e}")
    finally:
        if loaded_data_conn:
            loaded_data_conn.close()

###############################################################################
# Define function to map insignificant categorial variables to "others"
###############################################################################


def map_categorical_vars():
    '''
    This function maps all the insignificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py' 
    so that it can be imported as a variable in utils file.
    

    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.
    

    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already 
        exsists then the function replaces it.

    
    SAMPLE USAGE
        map_categorical_vars()
    '''
    city_mapped_conn = None
    try:
        city_mapped_conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        #if _is_table_already_populated(conn, "city_tier_mapped"):
        #    print("city_tier_mapped already populated")
        #    return

        print("Loading city_tier_mapped table")

        df_city_mapped = pd.read_sql("select * from city_tier_mapped", city_mapped_conn)

        # all the levels below 90 percentage are assigned to a single level called others
        new_df = df_city_mapped[
            ~df_city_mapped["first_platform_c"].isin(LIST_PLATFORM)]  # get rows for levels which are not present in LIST_PLATFORM
        new_df["first_platform_c"] = "others"  # replace the value of these levels to others
        old_df = df_city_mapped[
            df_city_mapped["first_platform_c"].isin(LIST_PLATFORM)]  # get rows for levels which are present in LIST_PLATFORM
        df_city_mapped = pd.concat([new_df, old_df])  # concatenate new_df and old_df to get the final dataframe

        # all the levels below 90 percentage are assigned to a single level called others
        new_df = df_city_mapped[
            ~df_city_mapped["first_utm_medium_c"].isin(LIST_MEDIUM)]  # get rows for levels which are not present in LIST_MEDIUM
        new_df["first_utm_medium_c"] = "others"  # replace the value of these levels to others
        old_df = df_city_mapped[df_city_mapped["first_utm_medium_c"].isin(LIST_MEDIUM)]  # get rows for levels which are present in LIST_MEDIUM
        df_city_mapped = pd.concat([new_df, old_df])  # concatenate new_df and old_df to get the final dataframe

        # all the levels below 90 percentage are assigned to a single level called others
        new_df = df_city_mapped[
            ~df_city_mapped["first_utm_source_c"].isin(LIST_SOURCE)]  # get rows for levels which are not present in LIST_SOURCE
        new_df["first_utm_source_c"] = "others"  # replace the value of these levels to others
        old_df = df_city_mapped[df_city_mapped["first_utm_source_c"].isin(LIST_SOURCE)]  # get rows for levels which are present in LIST_SOURCE
        df_city_mapped = pd.concat([new_df, old_df])  # concatenate new_df and old_df to get the final dataframe

        df_city_mapped = df_city_mapped.drop_duplicates()

        print("Saves the processed dataframe in the db in a table named 'categorical_variables_mapped'")
        df_city_mapped.to_sql(name="categorical_variables_mapped", con=city_mapped_conn, if_exists="replace", index=False)

    except Exception as e:
        print(f"Exception thrown in city_tier_mapped : {e}")
    finally:
        if city_mapped_conn:
            city_mapped_conn.close()
    
    #df_lead_scoring = read_csv_file(CSV_FILE_PATH+CSV_FILE_NAME)
    df_lead_scoring = read_csv_file(CSV_FILE_PATH+CSV_FILE_NAME)
    # profile = ProfileReport(df_lead_scoring, title="Raw Data Summary")
    # profile.to_notebook_iframe()
    # profile.to_file(REPORT_PATH+PROFILE_REPORT_FILENAME)
    write_profile_report(df_lead_scoring, PROFILE_REPORT_FILENAME,REPORT_PATH)

    #  # Specify the file name
    # file_name = 'significant_categorical_level.py'

    # # Open the file in write mode and write the content to it
    # with open(file_name, 'w') as file:
    #     for column in ['first_platform_c', 'first_utm_medium_c', 'first_utm_source_c']:
    #         df_cat_freq = df_lead_scoring[column].value_counts()
    #         df_cat_freq = pd.DataFrame({'column':df_cat_freq.index, 'value':df_cat_freq.values})
    #         df_cat_freq['perc'] = df_cat_freq['value'].cumsum()/df_cat_freq['value'].sum()
    #         if 'platform' in column:
    #             list_platform=list(df_cat_freq.loc[df_cat_freq['perc']<=0.9].column)
    #             file_content="list_platform = "+list_platform
                   
    #         elif 'medium' in column:
    #             list_medium=list(df_cat_freq.loc[df_cat_freq['perc']<=0.9].column)
    #             file_content="list_medium = "+list_medium
    #         elif 'source' in column:
    #             list_source=list(df_cat_freq.loc[df_cat_freq['perc']<=0.9].column)
    #             file_content="list_source = "+list_source
    #         else:
    #             print(column,)
    #             print(list(df_cat_freq.loc[df_cat_freq['perc']<=0.9].column))
    #             print('\n') 
    #         file.write(file_content)
    

##############################################################################
# Define function that maps interaction columns into 4 types of interactions
##############################################################################
def interactions_mapping():
    '''
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file. 


    INPUTS
        DB_FILE_NAME: Name of the database file
        DB_PATH : path where the db file should be present
        INTERACTION_MAPPING : path to the csv file containing interaction's
                                   mappings
        INDEX_COLUMNS_TRAINING : list of columns to be used as index while pivoting and
                                 unpivoting during training
        INDEX_COLUMNS_INFERENCE: list of columns to be used as index while pivoting and
                                 unpivoting during inference
        NOT_FEATURES: Features which have less significance and needs to be dropped
                                 
        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our features list. It is recommended 
        that you use an if loop and check if 'app_complete_flag' is present in 
        'categorical_variables_mapped' table and if it is present pass a list with 
        'app_complete_flag' column, or else pass a list without 'app_complete_flag'
        column.

    
    OUTPUT
        Saves the processed dataframe in the db in a table named 
        'interactions_mapped'. If the table with the same name already exsists then 
        the function replaces it.
        
        It also drops all the features that are not requried for training model and 
        writes it in a table named 'model_input'

    
    SAMPLE USAGE
        interactions_mapping()
    '''
    interactions_mapping_conn = None
    try:
        interactions_mapping_conn = sqlite3.connect(DB_PATH + DB_FILE_NAME)
        print("Reading of categorical_variables_mapped data ...")
        df = pd.read_sql("select * from categorical_variables_mapped", interactions_mapping_conn)
        print("Reading of categorical_variables_mapped data completed")

        df_interactions_mapping = pd.read_csv(INTERACTION_MAPPING+INTERACTION_FILE_NAME, index_col=[0])
        # unpivot the interaction columns and put the values in rows
        df_unpivot = pd.melt(df, id_vars=INDEX_COLUMNS, var_name='interaction_type', value_name='interaction_value')
        
        # handle the nulls in the interaction value column
        df_unpivot['interaction_value'] = df_unpivot['interaction_value'].fillna(0)
        # map interaction type column with the mapping file to get interaction mapping
        df = pd.merge(df_unpivot, df_interactions_mapping, on='interaction_type', how='left')

        #dropping the interaction type column as it is not needed
        df = df.drop(['interaction_type'], axis=1)
        # pivoting the interaction mapping column values to individual columns in the dataset
        df_pivot = df.pivot_table(
                values='interaction_value', index=INDEX_COLUMNS, columns='interaction_mapping', aggfunc='sum')
        df_pivot = df_pivot.reset_index()

        # profile = ProfileReport(df_pivot, title="Cleaned Data Summary")
        # profile.to_notebook_iframe()
        # profile.to_file(REPORT_PATH+CLEANED_REPORT_FILENAME)
        # generate the profile report of final dataset
        write_profile_report(df_pivot,CLEANED_REPORT_FILENAME,REPORT_PATH)
        # the file is written in the data folder
        df_pivot.to_csv(CSV_FILE_PATH+CLEANED_CSV_FILE_NAME, index=False)
        
        df_pivot.to_sql(name='interactions_mapped', con=interactions_mapping_conn, if_exists='replace', index=False)
        #print("df_pivot  COLUMNS are: ",df_pivot.columns)
        df_pivot.drop(columns=NOT_FEATURES, inplace=True)
        df_pivot.to_sql(name='model_input', con=interactions_mapping_conn, if_exists='replace', index=False)
        #print(df_pivot.columns)
    except Exception as e:
        print(f"Exception thrown in interactions_mapping : {e}")
    finally:
        if interactions_mapping_conn:
            interactions_mapping_conn.close()