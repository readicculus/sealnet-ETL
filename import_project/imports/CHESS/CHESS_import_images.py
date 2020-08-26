import logging
import os

import mlflow

from import_project import log_file_base
from import_project.imports import experiment
from import_project.imports.CHESS import SURVEY
from import_project.imports.CHESS.CHESSDataset import chess_datasets
from import_project.imports.deletions import delete_cam_images, delete_cam_labels
from import_project.utils.ingest_util import setup_logger, append_meta
from noaadb import Session
from noaadb.schema.utils.queries import add_or_get_cam_flight_survey
from thebook.print.func import printProgressBar
# TODO currently only works for EO
def process_cam(s, dataset, cam):
    logging.info("=== Processing %s %s ===" % (dataset.id(), cam))
    matches = dataset.get_cam_eo_ir_meta_matches(cam)

    total = len(matches)
    logging.info("%d matches" % (total))
    mlflow.log_param('directory_eo',os.path.join(dataset.color_dir, cam))
    mlflow.log_param('directory_ir',os.path.join(dataset.ir_dir, cam))
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
    return eo_ct, ir_ct



def import_images(dataset):
    delete_first = True
    if not os.path.exists(log_file_base):
        os.makedirs(log_file_base)
    s = Session()
    with mlflow.start_run(run_name='import_flight', experiment_id=experiment.experiment_id) as mlrun:
        # print('runId %s' % mlrun.info.run_id)
        # print('parent %s' % mlflow.get_run(mlrun.info.run_id).data.tags['mlflow.parentRunId'])
        mlflow.log_param('flight', dataset.flight)
        mlflow.log_param('survey', SURVEY)
        print("Processing %s" % dataset.flight)
        cams = dataset.get_cam_names()
        eo_total = 0
        ir_total = 0
        for cam in cams:
            # if cam =='P': continue
            lf = os.path.join(log_file_base, 'ingest_imagery_%s%s.log' % (dataset.id(), cam))
            setup_logger(lf)
            if delete_first:
                with mlflow.start_run(run_name='delete_cam', nested=True) as del_run:
                    # print('runId %s' % del_run.info.run_id)
                    # print('parent %s' % mlflow.get_run(del_run.info.run_id).data.tags['mlflow.parentRunId'])
                    delete_cam_labels(s, dataset, cam, SURVEY)
                    delete_cam_images(dataset, cam, SURVEY)
            with mlflow.start_run(run_name='import_cam', nested=True) as import_run:
                print("Cam %s" % cam)
                eo_ct, ir_ct = process_cam(s, dataset, cam)
                eo_total += eo_ct
                ir_total += ir_ct
            if os.path.exists(lf):
                os.remove(lf)
        mlflow.log_metric('eo_images', eo_total)
        mlflow.log_metric('ir_images', ir_total)
        print()

    if not os.path.exists(log_file_base):
        os.makedirs(log_file_base)
    s.close()

if __name__ == '__main__':
    # with mlflow.start_run(run_name='import_kotz', experiment_id=experiment.experiment_id) as mlrun_parent:
    for dataset in chess_datasets:
        import_images(dataset)
