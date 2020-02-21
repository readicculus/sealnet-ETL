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

lines = []
img_idx = 0
for im in images:
    image = images[im]["image"]
    hotspots = images[im]["hotspots"]
    x=1

    data = {}
    data["name"] = os.path.join("/fast/s3/images/rgb/",image.file_name)
    data["url"] = os.path.join("/fast/s3/images/rgb/",image.file_name)
    data["im_width"] = image.width
    data["im_height"] = image.height
    data["im_depth"] = image.depth
    data["labels"] = []
    img_idx +=1
    # data["attributes"] = {
    #     "Foggy": image.foggy
    # }
    data["attributes"] = None
    data["timestamp"] = 1000
    data["videoName"] = ""
    # data["index"] = img_idx

    worker = None
    timestamp = None
    job = None
    meta = []
    label_ids = []
    i = 0
    for (hotspot, eo_label, ir_label, ir_image, species, eo_job, eo_worker) in hotspots:
        worker=eo_worker
        x1, x2, y1, y2 = eo_label.x1, eo_label.x2, eo_label.y1, eo_label.y2

        annotation = {
          "id": i,
          "category": species.name,
          "attributes": {
              "Truncated": False,
              "Finalized": False,
              "Foggy": image.foggy
          },
          "label_id": eo_label.id,
          "manualShape": False,
            "box2d": {
                "x1": x1,
                "x2": x2,
                "y1": y1,
                "y2": y2
            },
            "poly2d": None,
            "box3d": None
        }
        i+=1
        data["labels"].append(annotation)
    #     timestamp = eo_label.start_date
    #     job = eo_job
    #     label_ids.append(eo_label.id)
    # time =  timestamp.strftime("%Y-%m-%d")
    lines.append(data)

    # data["%s-metadata"%label_attribute] = {
    #     "job-name": job.job_name,
    #     "class-map": species_map,
    #     "human-annotated": "yes" if (worker.human is None or worker.human) else "no",
    #     "objects": meta,
    #     "creation-date": time,
    #     "type": "groundtruth/object-detection",
    #     "img_quality": image.quality,
    #     "img_foggy": image.foggy,
    #     "label_ids": label_ids
    # }

s.close()
with open("out.json" , "w") as f:
    json.dump(lines, f, indent=4, sort_keys=True)
    f.write("\r\n")
