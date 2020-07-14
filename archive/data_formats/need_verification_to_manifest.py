import json

import boto3
from sqlalchemy import or_, and_, not_
from sqlalchemy.orm import aliased

from noaadb.schema.models import NOAAImage, TruePositiveLabels, Worker, Species, EOIRLabelPair
import os

from noaadb.api import LabelDBApi

out_file_name = "labels-need-verification"
label_attribute = "bounding-box"
file_path_base = "s3://noaa-data/images/rgb/compressed/"
# file_path_base = "/fast/s3/"

def get_all_hotspots(session):
    eo_label = aliased(TruePositiveLabels)
    eo_image = aliased(NOAAImage)
    eo_worker = aliased(Worker)
    species = aliased(Species)

    y = session.query(TruePositiveLabels, EOIRLabelPair) \
        .outerjoin(EOIRLabelPair, EOIRLabelPair.eo_label_id == TruePositiveLabels.id)\
        .join(species, TruePositiveLabels.species)\
        .join(eo_worker, TruePositiveLabels.worker)\
        .join(eo_image, TruePositiveLabels.image)\
        .join(TruePositiveLabels.job)\
        .filter(
        and_(
            eo_image.type == 'RGB',
            not_(TruePositiveLabels.is_shadow),
            species.name.in_(('Polar Bear', 'Ringed Seal', 'Bearded Seal', 'UNK Seal')),
            or_(
                eo_worker.name == 'noaa',
                TruePositiveLabels.end_date != None,
                TruePositiveLabels.x1 < 0,
                TruePositiveLabels.x2 > eo_image.width,
                TruePositiveLabels.y1 < 0,
                TruePositiveLabels.y2 > eo_image.height
            )
        )
    )\
    .all()
    return y

api = LabelDBApi()
api.begin_session()

hs_res = api.get_eo_labels(verification_only=True)
test = api.get_hotspots()
# s = Session()
# hs_res=get_all_hotspots(s)

# group hotspots by image
images = {}
for label, hotspot in hs_res:
    if not label.image.file_name in images:
        images[label.image.file_name] = {"image": label.image, "hotspots":[]}
    images[label.image.file_name]["hotspots"].append((label, hotspot))
species = api.get_all_species()
species_map = {}
for sp in species:
    species_map[sp.id-1] = sp.name

manifest_lines = []
for im in images:
    image = images[im]["image"]
    hotspots = images[im]["hotspots"]
    x=1

    data = {}
    data["source-ref"] = os.path.join(file_path_base, image.file_name)
    data[label_attribute] = {
        "annotations": [],
        "image_size": [{
            "width": image.width,
            "height": image.height,
            "depth": image.depth
        }]}
    worker = None
    timestamp = None
    job = None
    meta = []
    label_ids = []
    for (eo_label,hotspot) in hotspots:
        worker=eo_label.worker
        x1, x2, y1, y2 = eo_label.x1, eo_label.x2, eo_label.y1, eo_label.y2
        w = x2 - x1
        h = y2 - y1
        cx = x1 + w / 2
        cy = y1 + h / 2
        #https://docs.aws.amazon.com/sagemaker/latest/dg/sms-ui-template-crowd-bounding-box.html
        annotation = {
          "class_id": eo_label.species.id-1,
          "width": w,
          "top": y1,
          "height": h,
          "left": x1,
           "label_id": eo_label.id
        }
        meta.append({"confidence": 0 if eo_label.confidence is None else eo_label.confidence, "label_id": eo_label.id})
        data[label_attribute]["annotations"].append(annotation)
        timestamp = eo_label.start_date
        job = eo_label.job
        label_ids.append(eo_label.id)
    time =  timestamp.strftime("%Y-%m-%d")

    data["%s-metadata"%label_attribute] = {
        "job-name": job.job_name,
        "class-map": species_map,
        "human-annotated": "yes" if (worker.human is None or worker.human) else "no",
        "worker": worker.name,
        "objects": meta,
        "creation-date": time,
        "type": "groundtruth/object-detection",
        "img_quality": image.quality,
        "img_foggy": image.foggy,
        "label_ids": label_ids
    }

    manifest_lines.append(data)
api.close_session()
with open("%s.manifest" % out_file_name, "w") as f:
    for data in manifest_lines:
        json.dump(data, f, sort_keys=True)
        f.write("\r\n")
#

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
S3_BUCKET = "noaa-data"
manifest_file_name = "%s.manifest" % out_file_name
s3_dest_path = 'jobs/adjustment/%s'% manifest_file_name
s3.meta.client.upload_file(manifest_file_name, 'noaa-data', s3_dest_path)
print("s3://noaa-data/%s"%s3_dest_path)
