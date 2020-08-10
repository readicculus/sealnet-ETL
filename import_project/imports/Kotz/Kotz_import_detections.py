import logging
import os

import h5py
import mlflow
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from import_project import log_file_base
from import_project.imports import experiment
from import_project.imports.Kotz.KotzCSVRow import KotzCSVRow
from import_project.imports.Kotz.KotzDataset import fl07_dataset, fl06_dataset, fl05_dataset, fl04_dataset, fl01_dataset
from import_project.imports.Kotz.Kotz_import_images import SURVEY
from import_project.imports.deletions import delete_cam_labels
from import_project.imports.import_detections import process_labels
from import_project.utils.ingest_util import setup_logger
from import_project.utils.util import printProgressBar
from noaadb import Session, DATABASE_URI
from noaadb.schema.models import \
    Camera, Flight, Survey, Homography
from noaadb.schema.models.label_data import LabelType
from noaadb.schema.utils.queries import add_job_if_not_exists, add_worker_if_not_exists
from noaadb.schema.utils.schema_ops import drop_label_schema, create_label_schema


class RegisteredDetections():
    def __init__(self, dataset, eo_worker, job, cam):
        self.eo_file = dataset.get_cam_eo_detections_file(cam)
        self.ir_file = dataset.get_cam_ir_detections_file(cam)
        self.eo_worker = eo_worker
        self.transform_h5 = dataset.get_cam_transform(cam)
        self.ir_worker = os.path.basename(self.transform_h5) if self.transform_h5 else None
        self.job = job
        self.dataset = dataset
        self.name = '%s_%s' % (self.dataset.id(), cam)
        self.cam = cam

        self.data, self.has_ir = self.dataset.get_eo_ir_merged_detections(self.cam)

        not_correct = self.data.loc[self.data['species'] == 'incorrect']
        self.verified_incorrect = not_correct.loc[not_correct['confidence_eo'] >= .5]
        self.verified_correct = self.data.loc[self.data['species'] != 'incorrect']

        self.job = None
        self.eo_worker = None
        self.ir_worker = None

    def print_info(self):
        print(self.name)
        print("%d detections correct" % len(self.verified_correct))
        print("%d detections incorrect" % len(self.verified_incorrect))
        print("unique_species: %s" % self.verified_correct.species.unique())
        print("Has IR:  " + str(self.has_ir))
        print()

    def cam_id(self):
        return self.dataset.get_cam_id(self.cam)

    def create_job(self, s):
        self.job = add_job_if_not_exists(s, "test_flight_kotz_2019_review", self.eo_file)
        self.eo_worker = add_worker_if_not_exists(s, "Yolo/Gavin", True)
        if self.has_ir:
            self.ir_worker = add_worker_if_not_exists(s, "Projected VIAME", False)
            cam = s.query(Camera).filter_by(cam_name=self.dataset.get_cam_id(self.cam)) \
                .join(Flight).filter_by(flight_name=self.dataset.id()) \
                .join(Survey).filter_by(name=SURVEY).first()

            H = s.query(Homography).filter_by(file_name=os.path.basename(self.transform_h5)).filter_by(
                camera_id=cam.id).first()
            if not H:
                with h5py.File(self.transform_h5, "r") as f:
                    # List all groups
                    data = f['/']['TransformGroup']['0']['TransformParameters']
                    affine = np.array(data.value)
                    h00, h10, h01, h11, h02, h12 = affine
                    H = Homography(h00=h00, h01=h01, h02=h02,
                                   h10=h10, h11=h11, h12=h12,
                                   h20=0., h21=0., h22=1.,
                                   file_name=os.path.basename(self.transform_h5),
                                   file_path=self.transform_h5,
                                   camera_id=cam.id)
                    s.add(H)
                    s.flush()
                logging.info("Added homography for cam %d" % cam.id)

    def get_rows(self):
        rows = []
        for i, row in self.verified_correct.iterrows():
            kotz_row = KotzCSVRow(row["species"], row["image_eo"], row["image_ir"],
                                  row["x1_eo"], row["x2_eo"], row["y1_eo"], row["y2_eo"], row["x1_ir"], row["x2_ir"],
                                  row["y1_ir"], row["y2_ir"],
                                  row["confidence_eo"], row["confidence_ir"])
            rows.append(kotz_row)
        return rows

    # def process_incorrect(self, s):
    #     process_labels(s, self.verified_incorrect, self.job, self.eo_worker, self.ir_worker, LabelType.FP)
    #
    # def process_correct(self, s):
    #     process_labels(s, self.verified_correct, self.job, self.eo_worker, self.ir_worker, LabelType.TP)

JOB = 'kotz_review'

fl07_L = RegisteredDetections(fl07_dataset, 'Yolo/Gavin', JOB, 'LEFT')
fl07_C = RegisteredDetections(fl07_dataset, 'Yolo/Gavin', JOB, 'CENT')
fl07_dets = [fl07_C, fl07_L]

fl06_L = RegisteredDetections(fl06_dataset, 'Yolo/Gavin', JOB, 'LEFT')
fl06_C = RegisteredDetections(fl06_dataset, 'Yolo/Gavin', JOB, 'CENT')
fl06_dets = [fl06_C, fl06_L]

fl05_C = RegisteredDetections(fl05_dataset, 'Yolo/Gavin', JOB, 'CENT')
fl05_L = RegisteredDetections(fl05_dataset, 'Yolo/Gavin', JOB, 'LEFT')
fl05_dets = [fl05_C, fl05_L]

fl04_C = RegisteredDetections(fl04_dataset, 'Yolo/Gavin', JOB, 'CENT')
fl04_L = RegisteredDetections(fl04_dataset, 'Yolo/Gavin', JOB, 'LEFT')
fl04_dets = [fl04_C, fl04_L]
fl01_C = RegisteredDetections(fl01_dataset, 'Yolo/Gavin', JOB, 'CENT')

registered_list = [fl07_L, fl07_C, fl06_L, fl06_C, fl05_C, fl05_L, fl04_C, fl04_L]#, fl01_C]



def import_cam_labels(s, cam_regsistered_pair):

    fl_cam = (cam_regsistered_pair.dataset.id(), cam_regsistered_pair.cam)
    mlflow.log_param('cam', cam_regsistered_pair.dataset.get_cam_id(cam_regsistered_pair.cam))
    mlflow.log_param('flight', cam_regsistered_pair.dataset.id())
    mlflow.log_param('survey', SURVEY)
    logging.info("=== Processing %s %s ===" % fl_cam)
    cam_regsistered_pair.print_info()
    cam_regsistered_pair.create_job(s)
    print("Correct labels (Verified)")
    labels = cam_regsistered_pair.get_rows()
    process_labels(s, labels, cam_regsistered_pair.job, cam_regsistered_pair.eo_worker, cam_regsistered_pair.ir_worker)
    # print("Incorrect labels (FP)")
    # registered_pair.process_incorrect(s)
    logging.info("=== COMPLETED %s %s ===" % fl_cam)

def add_all(flight_datasets):
    s = Session()
    if not os.path.exists(log_file_base):
        os.makedirs(log_file_base)
    # flight_datasets = [fl07_dets, fl06_dets, fl05_dets, fl04_dets]
    for cam_data in flight_datasets:
        with mlflow.start_run(run_name='import_labels') as mlrun:
            # set flight params
            temp = cam_data[0]
            mlflow.log_param('flight', temp.dataset.flight)
            for dets in cam_data:
                # setup logger
                fl_cam = (dets.dataset.id(), dets.cam)
                lf = os.path.join(log_file_base, 'detections_%s%s.log' % fl_cam)
                setup_logger(lf)

                # delete labels run
                with mlflow.start_run(run_name='delete_labels', nested=True):
                    delete_cam_labels(s, dets.dataset, dets.cam, SURVEY)

                # import labels run
                with mlflow.start_run(run_name='import_labels', nested=True):
                    import_cam_labels(s, dets)
                    mlflow.log_artifact(lf)

                # delete log file
                if os.path.exists(lf):
                    os.remove(lf)
    if not os.path.exists(log_file_base):
        os.makedirs(log_file_base)
    s.close()

def drop_create_label_schemas():
    engine = create_engine(DATABASE_URI)
    with mlflow.start_run(run_name='reset_label_schema', experiment_id=experiment.experiment_id):
        with mlflow.start_run(run_name='drop_label_schema', nested=True):
            # drop_ml_schema(engine)
            drop_label_schema(engine, tables_only=False)
        with mlflow.start_run(run_name='create_label_schema', nested=True):
            create_label_schema(engine, tables_only=False)
            # create_ml_schema(engine)


# drop_create_label_schemas()
add_all([[fl07_L]])
