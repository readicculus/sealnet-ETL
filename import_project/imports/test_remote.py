''' include user name and password to request '''
import mlflow

TRACKING_URI = 'http://mlflow:cyberjunk@ec2-54-212-196-193.us-west-2.compute.amazonaws.com:5000'
# ''' set up tracking uri '''
mlflow.set_tracking_uri(TRACKING_URI)
client = mlflow.tracking.MlflowClient(TRACKING_URI)
with mlflow.start_run() as mlrun:
    mlflow.log_param('parameter', 1)
    mlflow.log_metric('metric', 2)
    client.log_artifact(mlrun.info.run_id, '/home/yuval/Documents/XNOR/sealnet-ETL/import_project/temp_move/polarbears.csv')
x=1