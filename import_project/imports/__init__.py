from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import mlflow
import os
print(os.environ['MLFLOW_TRACKING_URI'])
experiment_name = 'noaadb_imports'
experiment = mlflow.get_experiment_by_name(experiment_name)
