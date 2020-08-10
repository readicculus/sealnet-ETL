import os

import boto3
import mlflow

from import_project import log_file_base
from import_project.imports import experiment
from import_project.imports.CHESS import SURVEY
from import_project.imports.CHESS.CHESSDataset import flights, CHESSDataset, color_dir, ir_dir
from import_project.imports.CHESS.ChessCSVRow import ChessCSVRow
from import_project.imports.deletions import delete_cam_labels
from import_project.imports.import_detections import process_labels
from import_project.utils.ingest_util import setup_logger
from noaadb import Session
from noaadb.schema.utils.queries import add_worker_if_not_exists, add_job_if_not_exists
from thebook.s3.S3Url import S3Url
from thebook.s3.func import read_csv
s3_url=S3Url('s3://yboss/mlflow/0/e219de98e9ca4dc2bb41bbaf5de7b717/artifacts/TrainingAnimals_WithSightings_updating_standardized_eo.csv')
s3_client = boto3.client('s3')
df = read_csv(s3_client, s3_url)
df['hs_id'] = df['hs_id'].astype(str)

def parse_rows(detections):
    rows = []
    for i, row in detections.iterrows():
        kotz_row = ChessCSVRow(row["species_eo"],
                               row["image_eo"], None,
                               row["x1_eo"], row["x2_eo"], row["y1_eo"], row["y2_eo"],
                               None, None, None, None,
                               row["confidence_eo"], None, hs_id=row['hs_id_eo'])
        rows.append(kotz_row)
    return rows

# chess_datasets = []
# for flight in flights:
#     dataset = CHESSDataset(color_dir, ir_dir, flight, eo_df=df)
#     cams = dataset.get_cam_names()
#     for cam in cams:
#         x=1

def add_all():
    s = Session()
    eo_worker = add_worker_if_not_exists(s, "NOAA/Yuval", True)
    job = add_job_if_not_exists(s, "chess_2016", '')

    if not os.path.exists(log_file_base):
        os.makedirs(log_file_base)
    for flight in flights:
        with mlflow.start_run(run_name='import_labels', experiment_id=experiment.experiment_id) as mlrun:
            dataset = CHESSDataset(color_dir, ir_dir, flight, eo_df=df)
            # set flight params
            mlflow.log_param('flight', flight)
            cams = dataset.get_cam_names()
            flight_eo_added_count = 0
            flight_ir_added_count = 0
            for cam in cams:
                # setup logger
                fl_cam = (flight, cam)
                lf = os.path.join(log_file_base, 'detections_%s%s.log' % fl_cam)
                setup_logger(lf)

                # delete labels run
                with mlflow.start_run(run_name='delete_labels', nested=True, experiment_id=experiment.experiment_id):
                    delete_cam_labels(s, dataset, cam, SURVEY)

                # import labels run
                with mlflow.start_run(run_name='import_labels', nested=True, experiment_id=experiment.experiment_id):
                    labels = parse_rows(dataset.get_cam_eo_detections(cam))
                    eo_added, ir_added = process_labels(s, labels, job, eo_worker, None)
                    flight_eo_added_count += eo_added
                    flight_ir_added_count += ir_added
                    mlflow.log_artifact(lf) # log the log file to the run

                # delete log file
                if os.path.exists(lf):
                    os.remove(lf)
    if not os.path.exists(log_file_base):
        os.makedirs(log_file_base)
    s.close()

add_all()
# for ds in chess_datasets:
#     for i, row in self.verified_correct.iterrows():
#         kotz_row = KotzCSVRow(row["species"], row["image_eo"], row["image_ir"],
#                               row["x1_eo"], row["x2_eo"], row["y1_eo"], row["y2_eo"], row["x1_ir"], row["x2_ir"],
#                               row["y1_ir"], row["y2_ir"],
#                               row["confidence_eo"], row["confidence_ir"])
#         rows.append(kotz_row)
#     return rows