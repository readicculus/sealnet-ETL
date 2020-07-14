import logging
import os

import mlflow

from import_project import log_file_base
from import_project.imports.datasets import fl04_dataset, fl06_dataset, fl07_dataset, fl05_dataset
from import_project.imports.delete_images import delete_cam_images
from import_project.utils.util import printProgressBar
from import_project.utils.ingest_util import setup_logger, append_meta
from noaadb import Session
from noaadb.schema.utils.queries import add_or_get_cam_flight_survey

SURVEY = 'test_kotz_2019'

def process_cam(s, dataset, cam):
    logging.info("=== Processing %s %s ===" % (dataset.id(), cam))
    matches = dataset.get_cam_eo_ir_meta_matches(cam)

    total = len(matches)
    logging.info("%d matches" % (total))
    mlflow.log_param('directory',os.path.join(dataset.dir, cam))
    mlflow.log_param('cam', dataset.get_cam_id(cam))
    mlflow.log_param('flight', dataset.flight)
    mlflow.log_param('survey', SURVEY)
    mlflow.log_metric('image_events_ct', len(matches))

    cam = add_or_get_cam_flight_survey(s,dataset.get_cam_id(cam), dataset.flight, SURVEY)
    s.commit()
    eo_ct = 0
    ir_ct = 0
    for i, match in enumerate(matches):
        missing_header = {'rgb': {"header": {"stamp": i, "frame_id": "missing", "seq": i}},
                          'ir': {"header": {"stamp": i, "frame_id": "missing", "seq": i}}}
        printProgressBar(i, total, prefix='Progress:', suffix='Complete', length=50)
        eo = match.get('eo')
        ir = match.get('ir')
        meta = match.get('meta')
        eo_added, ir_added = append_meta(s, meta, cam, eo, ir, missing_header)
        eo_ct += int(eo_added)
        ir_ct += int(ir_added)
        try:
            s.flush()
            s.commit()
        except Exception as e:
            s.rollback()
            print(e)
            print(match)
        if i % 400 == 0:
            s.commit()
            s.expunge_all()
    mlflow.log_metric('eo_images', eo_ct)
    mlflow.log_metric('ir_images', ir_ct)
    s.commit()
    s.expunge_all()
    logging.info("=== COMPLETED %s %s ===" % (dataset.id(), cam))




def import_images(dataset):
    delete_first = True
    if not os.path.exists(log_file_base):
        os.makedirs(log_file_base)
    s = Session()
    with mlflow.start_run(run_name='import_flight') as mlrun:
        mlflow.log_param('flight', dataset.flight)
        print("Processing %s" % dataset.flight)
        cams = dataset.get_cam_names()
        for cam in cams:

            lf = os.path.join(log_file_base, 'ingest_imagery_%s%s.log' % (dataset.id(), cam))
            setup_logger(lf)
            if delete_first:
                with mlflow.start_run(run_name='delete_cam', nested=True):
                    delete_cam_images(dataset, cam, SURVEY)
            with mlflow.start_run(run_name='import_cam', nested=True) as mlrun:
                print("Cam %s" % cam)
                process_cam(s, dataset, cam)
                mlflow.log_artifact(lf)
            if os.path.exists(lf):
                os.remove(lf)
        print()
    if os.path.exists(log_file_base):
        os.rmdir(log_file_base)
    s.close()

if __name__ == '__main__':
    datasets = [fl05_dataset, fl04_dataset, fl07_dataset, fl06_dataset]  # , fl01_dataset]
    for dataset in datasets:
        import_images(dataset)
