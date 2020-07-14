import logging
import os
from datetime import datetime

import h5py
import mlflow
import pandas as pd

from import_project import log_file_base
from import_project.imports.import_images import SURVEY
from noaadb import Session, DATABASE_URI
from noaadb.schema.models import \
    EOIRLabelPair, IRLabelEntry, EOLabelEntry, IRImage, EOImage, Camera, Flight, Survey, HeaderMeta, \
    LabelEntry, Homography
from noaadb.schema.models.label_data import LabelType
from noaadb.schema.utils.queries import add_job_if_not_exists, add_worker_if_not_exists, get_existing_eo_label, \
    get_existing_ir_label
from noaadb.schema.utils.schema_ops import drop_ml_schema, drop_label_schema, create_label_schema, create_ml_schema
from sqlalchemy import create_engine

from import_project.imports.datasets import fl07_dataset, fl06_dataset, fl05_dataset, fl04_dataset, fl01_dataset
from import_project.utils.ingest_util import append_species, setup_logger
from import_project.utils.util import printProgressBar
from sqlalchemy.exc import IntegrityError
import numpy as np

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

        eo_df = pd.read_csv(self.eo_file, header=None, comment='#')
        eo_df.columns = ['id', 'image', 'num_dets', 'x1', 'y1', 'x2', 'y2', 'confidence', 'idk', 'species', 'conf2']
        eo_df.columns = [str(col) + '_eo' for col in eo_df.columns]
        eo_df['image_eo'] = eo_df['image_eo'].str.replace('rgb.tif', 'rgb.jpg')
        if self.ir_file is None:
            self.data = eo_df
            self.has_ir = False
        else:
            self.has_ir = True
            ir_df = pd.read_csv(self.ir_file, header=None, comment='#')
            ir_df.columns = ['id', 'image', 'num_dets', 'x1', 'y1', 'x2', 'y2', 'conf2', 'idk', 'species', 'confidence']
            ir_df.columns = [str(col) + '_ir' for col in ir_df.columns]

            merged = pd.merge(eo_df, ir_df, left_on='id_eo', right_on='id_ir', how='left')
            merged.drop(['id_ir', 'conf2_ir', 'conf2_eo', 'species_ir', 'idk_ir', 'idk_eo', 'num_dets_ir'], axis=1,
                        inplace=True)
            # assert(len(merged) - merged.image_ir.isnull().sum() - len(ir_df) == 0)
            if len(merged) - merged.image_ir.isnull().sum() - len(ir_df) != 0:
                print("len(merged) - merged.image_ir.isnull().sum() - len(ir_df) = %d" % (
                            len(merged) - merged.image_ir.isnull().sum() - len(ir_df)))
            self.data = merged
            self.data.bucket_loc[self.data['confidence_ir'] > 1, 'confidence_ir'] = 1
            int_cols_ir = ['x1_ir', 'y1_ir', 'x2_ir', 'y2_ir']
            self.data[int_cols_ir] = self.data[int_cols_ir].round()

        self.data.bucket_loc[self.data['confidence_eo'] > 1, 'confidence_eo'] = 1
        int_cols_eo = ['x1_eo', 'y1_eo', 'x2_eo', 'y2_eo']
        self.data[int_cols_eo] = self.data[int_cols_eo].round()

        self.data.rename(columns={'species_eo': 'species', 'id_eo': 'id'}, inplace=True)
        not_correct = self.data.bucket_loc[self.data['species'] == 'incorrect']
        self.verified_incorrect = not_correct.bucket_loc[not_correct['confidence_eo'] >= .5]
        self.verified_correct = self.data.bucket_loc[self.data['species'] != 'incorrect']

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

    def delete(self, s):
        eo_labels = s.query(LabelEntry).join(EOImage).join(HeaderMeta).join(Camera).filter_by(
            cam_name=self.dataset.get_cam_id(self.cam)) \
            .join(Flight).filter_by(flight_name=self.dataset.id()) \
            .join(Survey).filter_by(name=SURVEY).all()
        ir_labels = s.query(LabelEntry).join(IRImage).join(HeaderMeta).join(Camera).filter_by(
            cam_name=self.dataset.get_cam_id(self.cam)) \
            .join(Flight).filter_by(flight_name=self.dataset.id()) \
            .join(Survey).filter_by(name=SURVEY).all()
        for l in eo_labels + ir_labels:
            s.delete(l)
        s.commit()
        s.flush()

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
    def process_incorrect(self, s):
        process_labels(s, self.verified_incorrect, self.job, self.eo_worker, self.ir_worker, LabelType.FP)

    def process_correct(self, s):
        process_labels(s, self.verified_correct, self.job, self.eo_worker, self.ir_worker, LabelType.TP)

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


def add_label(s, row, im, worker, job, species, age_class, type):
    iseo = (type == 'eo')
    LabelClass = EOLabelEntry if iseo else IRLabelEntry
    label_entry = LabelClass(
        image_id=im.file_name,
        x1=row['x1_eo'] if iseo else row['x1_ir'],
        x2=row['x2_eo'] if iseo else row['x2_ir'],
        y1=row['y1_eo'] if iseo else row['y1_ir'],
        y2=row['y2_eo'] if iseo else row['y2_ir'],
        species=species,
        age_class=age_class
    )
    check = get_existing_eo_label(s, label_entry) if iseo else get_existing_ir_label(s, label_entry)
    logging.info("Label exists im: %s %d %d %d %d" % (im.file_name, label_entry.x1, label_entry.y1, label_entry.x2, label_entry.y2))
    if check is not None:
        return check
    label_entry = LabelClass(
        image_id=im.file_name,
        x1=row['x1_eo'] if iseo else row['x1_ir'],  # TODO set earlier
        x2=row['x2_eo'] if iseo else row['x2_ir'],
        y1=row['y1_eo'] if iseo else row['y1_ir'],
        y2=row['y2_eo'] if iseo else row['y2_ir'],
        confidence=row['confidence_eo' if iseo else 'confidence_ir'],
        start_date=datetime.now(),
        end_date=None,
        is_shadow=None,
        worker=worker,
        job=job,
        species=species,
        age_class=age_class
    )
    s.add(label_entry)
    try:
        s.flush()
        logging.info("SUCCESS: added label entry im: %s %d %d %d %d" % (
        im.file_name, label_entry.x1, label_entry.y1, label_entry.x2, label_entry.y2))
    except IntegrityError as e:
        logging.error("ERROR: adding label entry im: %s %d %d %d %d" % (
        im.file_name, label_entry.x1, label_entry.y1, label_entry.x2, label_entry.y2))
        print(e)
        s.rollback()
        return None

    return label_entry


def process_labels(s, rows, job, eo_worker, ir_worker, disc):
    species_map = {'unknown_seal': 'UNK Seal',
                   'unknown_pup': 'UNK Seal',
                   'ringed_seal': 'Ringed Seal',
                   'ringed_pup': 'Ringed Seal',
                   'bearded_seal': 'Bearded Seal',
                   'bearded_pup': 'Bearded Seal',
                   'animal': 'animal',
                   'Ringed Seal': 'Ringed Seal',
                   'Bearded Seal': 'Bearded Seal',
                   'Polar Bear': 'Polar Bear',
                   'incorrect': 'falsepositive'}

    total = len(rows)
    num_ir_missing = 0
    j = 0
    for i, row in rows.iterrows():
        if j % 10 == 0:
            printProgressBar(j, total, prefix='Progress:', suffix='Complete', length=50)
        j += 1
        species_id = "UNK" if pd.isnull(row.species) else species_map[row.species]
        is_pup = not pd.isnull(row.species) and 'pup' in row.species
        age_class = "Pup" if is_pup else "Adult"
        if species_id == 'falsepositive':
            age_class = None
        species = append_species(s, species_id)

        im_eo = s.query(EOImage).filter_by(file_name=os.path.basename(row["image_eo"])).first()
        label_entry_eo, label_entry_ir = None, None

        if im_eo is None:
            raise Exception("ERROR %s" % os.path.basename(row["image_eo"]))
        else:
            label_entry_eo = add_label(s, row, im_eo, eo_worker, job, species, age_class, 'eo')

        if ir_worker:
            if pd.isna(row["image_ir"]):  # no ir match for this image
                num_ir_missing += 1
            else:
                im_ir = s.query(IRImage).filter_by(file_name=os.path.basename(row["image_ir"])).first()
                if im_ir is None:
                    print("ERROR %s" % os.path.basename(row["image_ir"]))
                else:
                    label_entry_ir = add_label(s, row, im_ir, ir_worker, job, species, age_class, 'ir')

        sighting = EOIRLabelPair(
            eo_label=label_entry_eo,
            ir_label=label_entry_ir
        )
        s.add(sighting)
        try:
            s.flush()
            logging.info("SUCCESS: added sighting eo:%s ir:%s" % (label_entry_eo is not None, label_entry_ir is not None))
        except IntegrityError as e:
            logging.error("FAILED: added sighting eo:%s ir:%s" % (label_entry_eo is not None, label_entry_ir is not None))
            logging.error(e)
            print(e)
            s.rollback()

        if j % 100 == 0:
            printProgressBar(j, total, prefix='Progress:', suffix='Committing', length=50)
            s.commit()
            s.flush()
    s.commit()
    s.flush()
    print("%d images have no IR/not aligned" % num_ir_missing)

def add(cam_regsistered_pair):
    fl_cam = (cam_regsistered_pair.dataset.id(), cam_regsistered_pair.cam)
    lf = os.path.join(log_file_base, 'detections_%s%s.log' % fl_cam)
    setup_logger(lf)
    logging.info("=== Processing %s %s ===" % fl_cam)
    cam_regsistered_pair.print_info()
    cam_regsistered_pair.create_job(s)
    print("Correct labels (Verified)")
    cam_regsistered_pair.process_correct(s)
    # print("Incorrect labels (FP)")
    # registered_pair.process_incorrect(s)
    logging.info("=== COMPLETED %s %s ===" % fl_cam)

def add_all():
    with mlflow.start_run(run_name='import_labels') as mlrun:
        for dets in fl05_dets:
            add(dets)


def drop_create_label_schemas():
    engine = create_engine(DATABASE_URI)
    with mlflow.start_run(run_name='reset_label_schema'):
        with mlflow.start_run(run_name='drop_label_schema', nested=True):
            drop_ml_schema(engine)
            drop_label_schema(engine)
            Homography.__table__.drop(bind=engine, checkfirst=True)
        with mlflow.start_run(run_name='create_label_schema', nested=True):
            create_label_schema(engine)
            create_ml_schema(engine)
            Homography.__table__.create(bind=engine, checkfirst=True)

s = Session()

add_all()
s.close()
