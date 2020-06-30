import os

from sqlalchemy import and_, engine
from sqlalchemy.orm import scoped_session, sessionmaker

from build.lib.noaadb import Session
from noaadb.schema.models import ImageType, NOAAImage, FlightCamId, FlightMetaEvent, FlightMetaInstruments, \
    FlightMetaHeader
from scripts.get_image_size import get_image_size
from scripts.ingest.kotz_2019.ingest_util import append_meta, image_fn_parser
from scripts.ingest.kotz_2019.datasets import kotz_datasets, fl07_dataset, fl06_dataset, fl05_dataset, fl04_dataset, \
    fl01_dataset
from scripts.util import printProgressBar

SURVEY = 'test_kotz_2019'



def process_cam(s, dataset, cam):
    matches = dataset.get_cam_eo_ir_meta_matches(cam)

    total = len(matches)
    fc_id = FlightCamId(flight=dataset.flight, cam=dataset.get_cam_id(cam), survey=SURVEY)
    s.merge(fc_id)
    s.flush()
    s.commit()
    for i, match in enumerate(matches):
        printProgressBar(i, total, prefix='Progress:', suffix='Complete', length=50)
        eo = match.get('eo')
        ir = match.get('ir')
        meta = match.get('meta')
        append_meta(s, meta, fc_id, eo, ir)

        try:
            s.flush()
        except Exception as e:
            s.rollback()
            print(e)
            print(match)
        if i % 400 == 0:
            s.commit()
            s.expunge_all()
    s.commit()
    s.expunge_all()

def try_delete(dataset, cam):
    tries = 0
    while tries < 5:
        tries+=1
        s2 = Session()
        try:
            fc_id = s2.query(FlightCamId).filter(and_(FlightCamId.flight == dataset.flight,
                                            and_(FlightCamId.cam == dataset.get_cam_id(cam),
                                                 FlightCamId.survey == SURVEY))).delete()

            s2.commit()
            print("Deleted %d FlightCamId" % fc_id)
            s2.close()
            return True
        except Exception as e:
            s2.rollback()
            s2.close()
            print(e)
            print("rolling back")

    return False


Session.configure(autoflush=False, expire_on_commit=False)
s = Session()

datasets = [fl07_dataset, fl06_dataset, fl05_dataset, fl04_dataset, fl01_dataset]
# datasets = [fl01_dataset, fl07_dataset]
delete_first = True
for dataset in datasets:
    print("Processing %s" %dataset.flight)
    cams = dataset.get_cam_names()
    for cam in cams:
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