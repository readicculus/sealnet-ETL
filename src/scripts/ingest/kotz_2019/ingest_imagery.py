import logging
import os

from noaadb import Session
from noaadb.schema.models import Flight, Survey, Camera
from noaadb.utils.queries import add_or_get_cam_flight_survey
from scripts.ingest.kotz_2019.ingest_util import append_meta, setup_logger, log_file_base
from scripts.ingest.kotz_2019.datasets import fl07_dataset, fl06_dataset, fl05_dataset, fl04_dataset, \
    fl01_dataset
from scripts.util import printProgressBar

SURVEY = 'test_kotz_2019'

def process_cam(s, dataset, cam):
    logging.info("=== Processing %s %s ===" % (dataset.id(), cam))
    matches = dataset.get_cam_eo_ir_meta_matches(cam)

    total = len(matches)
    logging.info("%d matches" % (total))

    cam = add_or_get_cam_flight_survey(s,dataset.get_cam_id(cam), dataset.flight, SURVEY)
    s.commit()

    for i, match in enumerate(matches):
        printProgressBar(i, total, prefix='Progress:', suffix='Complete', length=50)
        eo = match.get('eo')
        ir = match.get('ir')
        meta = match.get('meta')
        append_meta(s, meta, cam, eo, ir)

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
    s.commit()
    s.expunge_all()
    logging.info("=== COMPLETED %s %s ===" % (dataset.id(), cam))


def try_delete(dataset, cam):
    tries = 0
    while tries < 5:
        tries+=1
        s2 = Session()
        try:
            cam_obj = s2.query(Camera).filter_by(cam_name=dataset.get_cam_id(cam))\
                .join(Flight).filter_by(flight_name=dataset.flight)\
                .join(Survey).filter_by(name=SURVEY).first()
            if not cam_obj:
                s2.close()
                return True
            s2.delete(cam_obj)

            s2.commit()
            logging.info("Deleted %s %s %s" %(cam_obj.cam_name,cam_obj.flight.flight_name,cam_obj.flight.survey.name))
            s2.close()
            return True
        except Exception as e:
            logging.error("FAILED TO DELETE %s %s %s" %(cam_obj.cam_name,cam_obj.flight.flight_name,cam_obj.flight.survey.name))
            s2.rollback()
            s2.close()
            print(e)
            print("rolling back")

    return False


Session.configure(autoflush=False, expire_on_commit=False)
s = Session()

datasets = [fl07_dataset, fl06_dataset, fl05_dataset, fl04_dataset]#, fl01_dataset]
# datasets = [fl01_dataset]
# datasets = [fl01_dataset, fl07_dataset]
delete_first = True
for dataset in datasets:
    print("Processing %s" %dataset.flight)
    cams = dataset.get_cam_names()

    for cam in cams:
        lf = os.path.join(log_file_base, 'ingest_imagery_%s%s.log' % (dataset.id(), cam))
        setup_logger(lf)

        if delete_first:
            try_delete(dataset, cam)

        print("Cam %s" % cam)
        process_cam(s, dataset, cam)
    print()

s.close()
#
# for meta in meta_list:
#     append_meta(s, meta)
# a=1