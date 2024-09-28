##############################################################################
# Import the necessary modules
##############################################################################
import pytest
from utils import *
from constants import *
import sqlite3



###############################################################################
# Write test cases for load_data_into_db() function
# ##############################################################################

def test_load_data_into_db():
    """_summary_
    This function checks if the load_data_into_db function is working properly by
    comparing its output with test cases provided in the db in a table named
    'loaded_data_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_get_data()

    """
    # build_dbs()
    # load_data_into_db()
    # connection = sqlite3.connect(DB_PATH + DB_FILE_NAME)
    # df = pd.read_sql('select * from loaded_data', connection)
    # connection.close()
    # connection = sqlite3.connect(UNIT_TEST_DB_FILE_NAME)
    # ref_df = pd.read_sql('select * from loaded_data_test_case', connection)
    #load_data_into_db(DB_PATH, DB_FILE_NAME, DATA_DIRECTORY)
    load_data_into_db()
    
    print("Connecting to database")
    cnx = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    print("Reading data from loaded_data table")
    loaded_data = pd.read_sql('select * from loaded_data', cnx)
    print("loaded_data shape: ", loaded_data.shape)
    printColumns(cnx,'loaded_data')  
    print("Connecting to database",TEST_DB_PATH+UNIT_TEST_DB_FILE_NAME)
    cnx_test = sqlite3.connect(TEST_DB_PATH+UNIT_TEST_DB_FILE_NAME)
    print("Reading data from loaded_data_test_case table") 
    loaded_data_test_case = pd.read_sql('select * from loaded_data_test_case', cnx_test)
    print("loaded_data_test_case shape: ", loaded_data_test_case.shape)
    printColumns(cnx_test,'loaded_data_test_case')   
   
    print("Closing database connections")
    cnx.close()
    cnx_test.close()
    
    assert loaded_data.equals(loaded_data_test_case), "Dataframes not same, incorrect data loading"
    
def printColumns(conn,table_name):  
    # Execute PRAGMA query to get column info
    cursor = conn.execute(f'PRAGMA table_info({table_name})')

    # Fetch all columns
    columns = cursor.fetchall()

    # Print column names and details
    for col in columns:
        print(f"Column ID: {col[0]}, Name: {col[1]}, Type: {col[2]}")
###############################################################################
# Write test cases for map_city_tier() function
# ##############################################################################
def test_map_city_tier():
    """_summary_
    This function checks if map_city_tier function is working properly by
    comparing its output with test cases provided in the db in a table named
    'city_tier_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_map_city_tier()

    """
    
    
###############################################################################
# Write test cases for map_categorical_vars() function
# ##############################################################################    
def test_map_categorical_vars():
    """_summary_
    This function checks if map_cat_vars function is working properly by
    comparing its output with test cases provided in the db in a table named
    'categorical_variables_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'
    
    SAMPLE USAGE
        output=test_map_cat_vars()

    """    
    

###############################################################################
# Write test cases for interactions_mapping() function
# ##############################################################################    
def test_interactions_mapping():
    """_summary_
    This function checks if test_column_mapping function is working properly by
    comparing its output with test cases provided in the db in a table named
    'interactions_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_column_mapping()

    """ 
   
