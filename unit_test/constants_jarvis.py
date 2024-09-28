# You can create more variables according to your project. The following are the basic variables that have been provided to you
ROOT_PATH='/airflow/dags/Lead_scoring_data_pipeline'

DATA_DIRECTORY = f"{ROOT_PATH}/data"
DB_PATH = f"{ROOT_PATH}/database"
DB_FILE_NAME = "utils_output.db"

INTERACTION_MAPPING = f"{ROOT_PATH}/mapping"
INTERACTION_FILE_NAME = 'interaction_mapping.csv'
INDEX_COLUMNS_TRAINING = []
INDEX_COLUMNS_INFERENCE = []
NOT_FEATURES = ['created_date']
INDEX_COLUMNS = ["created_date","city_tier","first_platform_c","first_utm_medium_c","first_utm_source_c","total_leads_droppped","referred_lead","app_complete_flag"]
     
CSV_FILE_NAME = 'leadscoring.csv'
CSV_FILE_PATH=f"{ROOT_PATH}/data"

CLEANED_CSV_FILE_NAME = 'cleaned_data.csv'

REPORT_PATH=f"{ROOT_PATH}/profile_report"
PROFILE_REPORT_FILENAME='raw_data_report.html'
CLEANED_REPORT_FILENAME='cleaned_data_report.html'

MAPS_FILE_PATH = f"{ROOT_PATH}/mapping"
MAPS_FILE_NAME = 'interaction_mapping.csv'
