''' include user name and password to request '''
import mlflow


with mlflow.start_run() as mlrun:
    mlflow.log_param('parameter', 1)
    mlflow.log_metric('metric', 2)
    client.log_artifact(mlrun.info.run_id, '/home/yuval/Documents/XNOR/sealnet-ETL/import_project/temp_move/polarbears.csv')
x=1