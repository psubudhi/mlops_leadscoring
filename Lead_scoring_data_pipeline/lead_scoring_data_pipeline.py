##############################################################################
# Import necessary modules
# #############################################################################


from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from datetime import datetime, timedelta
from Lead_scoring_data_pipeline.utils import *
from Lead_scoring_data_pipeline.data_validation_checks import *
from Lead_scoring_data_pipeline.constants import *
from Lead_scoring_data_pipeline.mapping.city_tier_mapping import *
from Lead_scoring_data_pipeline.mapping.significant_categorical_level import *
from Lead_scoring_data_pipeline.schema import *
from datetime import date
import mlflow
import mlflow.sklearn

###############################################################################
# Define default arguments and DAG
###############################################################################

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022,7,30),
    'retries' : 1, 
    'retry_delay' : timedelta(seconds=5)
}


ML_data_cleaning_dag = DAG(
                dag_id = 'Lead_Scoring_Data_Engineering_Pipeline',
                default_args = default_args,
                description = 'DAG to run data pipeline for lead scoring',
                schedule_interval = '@daily',
                catchup = False
)


#make sure to run mlflow server before this. 
experiment_name = mlflow_experiment_name+'_'+date.today().strftime("%d_%m_%Y")+'_'+short_exp_name_identifier
mlflow.set_tracking_uri(mlflow_tracking_uri)

try:
    # Creating an experiment
    logging.info("Creating mlflow experiment")
    mlflow.create_experiment(experiment_name)
except:
    pass
# Setting the environment with the created experiment
mlflow.set_experiment(experiment_name)

###############################################################################
# Create a task for build_dbs() function with task_id 'building_db'
###############################################################################

build_dbs_operator = PythonOperator(
            task_id = 'building_db',
            python_callable = build_dbs,
            dag = ML_data_cleaning_dag)



###############################################################################
# Create a task for raw_data_schema_check() function with task_id 'checking_raw_data_schema'
###############################################################################

checking_raw_data_schema_operator = PythonOperator(
            task_id = 'rawdata_schema_check',
            python_callable = raw_data_schema_check,
            dag = ML_data_cleaning_dag)    

###############################################################################
# Create a task for load_data_into_db() function with task_id 'loading_data'
##############################################################################

load_data_into_db_operator = PythonOperator(
            task_id = 'loading_data',
            python_callable = load_data_into_db,
            dag = ML_data_cleaning_dag)
            
###############################################################################
# Create a task for map_city_tier() function with task_id 'mapping_city_tier'
###############################################################################

mapping_city_tier_operator = PythonOperator(
            task_id = 'mapping_city_tier',
            python_callable = map_city_tier,
            dag = ML_data_cleaning_dag)


###############################################################################
# Create a task for map_categorical_vars() function with task_id 'mapping_categorical_vars'
###############################################################################

map_categorical_vars_operators = PythonOperator(
            task_id = 'map_categorical_vars',
            python_callable = map_categorical_vars,
            dag = ML_data_cleaning_dag)


###############################################################################
# Create a task for interactions_mapping() function with task_id 'mapping_interactions'
###############################################################################

interactions_mapping_operator = PythonOperator(
            task_id = 'mapping_interactions',
            python_callable = interactions_mapping,
            dag = ML_data_cleaning_dag)
            
###############################################################################
# Create a task for model_input_schema_check() function with task_id 'checking_model_inputs_schema'
###############################################################################

model_input_schema_check_operator = PythonOperator(
            task_id = 'model_input_schema_check',
            python_callable = model_input_schema_check,
            dag = ML_data_cleaning_dag)

###############################################################################
# Define the relation between the tasks
###############################################################################


build_dbs_operator >> checking_raw_data_schema_operator>>load_data_into_db_operator >> mapping_city_tier_operator >> map_categorical_vars_operators >> interactions_mapping_operator >> model_input_schema_check_operator
