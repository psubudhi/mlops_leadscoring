# You can create more variables according to your project. The following are the basic variables that have been provided to you
#ROOT_PATH='/home/Assignment'


DB_PATH = '/home/airflow/dags/unit_test/'
TEST_DB_PATH = f"{DB_PATH}"
DB_FILE_NAME = "utils_output.db"
UNIT_TEST_DB_FILE_NAME = "unit_test_cases.db"
DATA_DIRECTORY = ''
INTERACTION_MAPPING = '/home/Assignment/01_data_pipeline/notebooks/Maps/'
INTERACTION_FILE_NAME = 'interaction_mapping.csv'
INDEX_COLUMNS_TRAINING = []
INDEX_COLUMNS_INFERENCE = []
NOT_FEATURES = ['created_date']
INDEX_COLUMNS = ["created_date","city_tier","first_platform_c","first_utm_medium_c","first_utm_source_c","total_leads_droppped","referred_lead","app_complete_flag"]
     
CSV_FILE_NAME = 'leadscoring.csv'
CSV_FILE_PATH=f"{DB_PATH}"

CLEANED_CSV_FILE_NAME = 'cleaned_data.csv'

REPORT_PATH='/home/Assignment/01_data_pipeline/notebooks/profile_report/'
PROFILE_REPORT_FILENAME='raw_data_report.html'
CLEANED_REPORT_FILENAME='cleaned_data_report.html'

MAPS_FILE_PATH = '/home/Assignment/01_data_pipeline/notebooks/Maps/'
MAPS_FILE_NAME = 'interaction_mapping.csv'
LEADSCORING_TEST_CSV_PATH = f"{DATA_DIRECTORY}leadscoring_test.csv"
