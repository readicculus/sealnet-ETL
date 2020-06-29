import os

from build.lib.noaadb import Session
from noaadb.schema.models import ImageType, NOAAImage, FlightCamId
from scripts.get_image_size import get_image_size
from scripts.ingest.kotz_2019.ingest_util import append_meta, image_fn_parser
from scripts.ingest.kotz_2019.datasets import kotz_datasets
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

        # if eo is not None:
        #     add_image(s, eo, ImageType.RGB, rgb_obj, fc_id)
        #
        # if ir is not None:
        #     add_image(s, ir, ImageType.IR, ir_obj, fc_id)

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

Session.configure(autoflush=False, expire_on_commit=False)
s = Session()

datasets = kotz_datasets
for dataset in datasets:
    print("Processing %s" %dataset.flight)
    cams = dataset.get_cam_names()
    for cam in cams:
        print("Cam %s" % cam)
        process_cam(s, dataset, cam)
        print()
s.close()
#
# for meta in meta_list:
#     append_meta(s, meta)
# a=1