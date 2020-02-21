import json

import boto3
from sqlalchemy.orm import aliased

from noaadb import Session
from noaadb.queries import get_job_by_name, get_all_species, get_worker
from noaadb.models import NOAAImage, Label, Worker, Job, Species, Hotspot
import os

out_file_name = "polar-bear-compressed-images-test"
label_attribute = "bounding-box"
file_path_base = "s3://noaa-data/"
# file_path_base = "/fast/s3/"

def get_all_hotspots(session):
    eo = aliased(Label)
    ir = aliased(Label)
    eo_image = aliased(NOAAImage)
    ir_image = aliased(NOAAImage)
    eo_job = aliased(Job)
    eo_worker = aliased(Worker)

    y = session.query(Hotspot, eo, ir, eo_image, ir_image, Species, eo_job, eo_worker) \
        .outerjoin(ir, Hotspot.ir_label == ir.id)\
        .outerjoin(eo, Hotspot.eo_label == eo.id)\
        .outerjoin(eo_image, eo.image==eo_image.id)\
        .outerjoin(ir_image, ir.image==ir_image.id)\
        .outerjoin(Species, eo.species==Species.id)\
        .outerjoin(eo_job, eo_job.id==eo.job)\
        .outerjoin(eo_worker, eo.worker==eo_worker.id)\
        .filter(eo.is_shadow == False).all()
    return y

s = Session()
hs_res=get_all_hotspots(s)

# group hotspots by image
images = {}
for hotspot, eo_label, ir_label, eo_image, ir_image, species, eo_job, eo_worker in hs_res:
    if not eo_image.file_name in images:
        images[eo_image.file_name] = {"image": eo_image, "hotspots":[]}
    images[eo_image.file_name]["hotspots"].append((hotspot, eo_label, ir_label, ir_image, species, eo_job, eo_worker))
species = get_all_species(s)
species_map = {}
for sp in species:
    species_map[sp.id-1] = sp.name

manifest_lines = []
for im in images:
    image = images[im]["image"]
    hotspots = images[im]["hotspots"]
    x=1

    data = {}
    data["source-ref"] = os.path.join(file_path_base, image.file_path)
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
    for (hotspot, eo_label, ir_label, ir_image, species, eo_job, eo_worker) in hotspots:
        worker=eo_worker
        x1, x2, y1, y2 = eo_label.x1, eo_label.x2, eo_label.y1, eo_label.y2
        w = x2 - x1
        h = y2 - y1
        cx = x1 + w / 2
        cy = y1 + h / 2
        #https://docs.aws.amazon.com/sagemaker/latest/dg/sms-ui-template-crowd-bounding-box.html
        annotation = {
          "class_id": eo_label.species-1,
          "width": w,
          "top": y1,
          "height": h,
          "left": x1,
           "label_id": eo_label.id
        }
        meta.append({"confidence": 0 if eo_label.confidence is None else float(eo_label.confidence) / 100.0, "label_id": eo_label.id})
        data[label_attribute]["annotations"].append(annotation)
        timestamp = eo_label.start_date
        job = eo_job
        label_ids.append(eo_label.id)
    time =  timestamp.strftime("%Y-%m-%d")

    data["%s-metadata"%label_attribute] = {
        "job-name": job.job_name,
        "class-map": species_map,
        "human-annotated": "yes" if (worker.human is None or worker.human) else "no",
        "objects": meta,
        "creation-date": time,
        "type": "groundtruth/object-detection",
        "img_quality": image.quality,
        "img_foggy": image.foggy,
        "label_ids": label_ids
    }

    manifest_lines.append(data)
s.close()
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
