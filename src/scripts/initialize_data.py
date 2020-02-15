import datetime
import boto3
import botocore
import pandas as pd
import os
import numpy as np
from PIL import Image
from botocore.exceptions import ClientError
from sqlalchemy import create_engine
from datetime import datetime
from noaadb import DATABASE_URI, Session, queries
from noaadb.models import NOAAImage, Label, Worker, Job, Species, Hotspot
from noaadb.queries import species_exists, job_exists, worker_exists, get_image, get_job_by_name, get_worker, get_species
from noaadb.schema_ops import refresh_schema
from dateutil import parser
import pytz

if True:
    refresh_schema()

LOCAL_S3 = "/data/raw_data/PolarBears/s3_images/"
s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
S3_BUCKET = "noaa-data"

pb_df = pd.read_csv("polarbears.csv")
CHESS_PATH = "/data/raw_data/Polar_Bear/2016_Chukchi_CHESS_US/"
BEAUFORT_PATH = "/data/raw_data/PolarBears/s3_images/2019_Beaufort_PolarBears/"
RU_PATH = "/data/raw_data/Polar_Bear/2016_Chukchi_CHESS_Russia/"
BACKUP_PATH = "/data/raw_data/TrainingAnimals_ColorImages"
BACKUP_PATH_IR = "/data/raw_data/TrainingAnimals_ThermalImages"
def file_exists(path):
    if not pd.isna(path):
        return os.path.exists(path)
    return None

def key_exists(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return False
    return True

def parse_chess_filename (f):
    info = {}
    e = f.split('_')
    e = [a for a in e if a != ""]
    info['survey'] = e[0]
    info['flight'] = e[1]+"_"+e[2]
    info['camPos'] = e[3]
    info['timestamp'] = e[4]
    info['camtype'] = e[5].split('-')[0]
    return info

job_name = "polarbear_labels_v1.0"
job_path= "jobs/original_polarbear_labels.csv"


for i, row in pb_df.iterrows():
    vals = list(row)
    id = vals[0]
    rgb_image_name = vals[1]
    ir_image_name = None if pd.isna(vals[2]) else vals[2]
    hotspot_id = None if pd.isna(vals[3]) else int(vals[3])
    pb_id = None if pd.isna(vals[4]) else vals[4]
    hotspot_type = vals[5]
    species_id = vals[6]
    species_confidence = None if pd.isna(vals[7]) else int(vals[7].replace("%", ""))
    fog = vals[8]
    thermal_x = None if pd.isna(vals[9]) else vals[9]
    thermal_y = None if pd.isna(vals[10]) else vals[10]
    updated = vals[19]
    status = None if pd.isna(vals[20]) else vals[20].replace("none", "")
    x1 = vals[15] if updated else vals[11]
    y1 = vals[16] if updated else vals[12]
    x2 = vals[17] if updated else vals[13]
    y2 = vals[18] if updated else vals[14]
    # l_u = vals[15]
    # t_u = vals[16]
    # r_u = vals[17]
    # b_u = vals[18]


    if fog == "No":
        fog = False
    elif fog =="Yes":
        fog = True
    else:
        fog = None
    a = {}
    timestamp_obj = None
    if i < 36:
        # CHESS Rows
        path = CHESS_PATH
        a = parse_chess_filename(rgb_image_name)
        timestamp = datetime.strptime(a["timestamp"], "%Y%m%d%H%M%S.%f%Z")
        timestamp_str = timestamp.strftime("%d-%m-%Y %H:%M:%S GMT-4")
        timestamp_obj = parser.parse(timestamp_str)
    elif i < 222:
        # Beaufort Rows
        path = BEAUFORT_PATH

        e = rgb_image_name.split('_')
        e = [a for a in e if a != ""]
        a['survey'] = "BEAUFORT2019"
        a['flight'] = e[4]
        a['camPos'] = e[5]
        a['timestamp'] = e[6] + e[7]
        timestamp = datetime.strptime(a["timestamp"], "%Y%m%d%H%M%S.%f")
        timestamp_str = timestamp.strftime("%d-%m-%Y %H:%M:%S GMT-4")
        timestamp_obj = parser.parse(timestamp_str)
    else:
        # Russia rows
        path = RU_PATH

        e = rgb_image_name.split('_')
        e = [a for a in e if a != ""]
        a['survey'] = "CHESS-ru"
        a['flight'] = None
        a['camPos'] = e[4].split(".")[0]
        a['timestamp'] = e[2]
        timestamp = datetime.strptime(a["timestamp"], "%Y-%m-%d %H-%M-%S")
        timestamp_str = timestamp.strftime("%d-%m-%Y %H:%M:%S GMT-4")
        timestamp_obj = parser.parse(timestamp_str)
    rgb_path = os.path.join(path, rgb_image_name).strip()
    ir_path = None if ir_image_name is None else os.path.join(path, ir_image_name).strip()
    rgb_exists = file_exists(rgb_path)
    ir_exists = ir_image_name is not None and file_exists(ir_path)
    if not rgb_exists:
        rgb_path = os.path.join(BACKUP_PATH, rgb_image_name).strip()
        rgb_exists = file_exists(rgb_path)
    if ir_image_name is not None and not ir_exists:
        ir_path = os.path.join(BACKUP_PATH_IR, ir_image_name).strip()
        ir_exists = file_exists(ir_path)
    if not rgb_exists:
        print("Not found RGB!", rgb_image_name)
    if not ir_exists:
        print("Not found IR!", ir_image_name)


    worker_name = "yuval" if updated else "noaa"

    rgb_im = Image.open(rgb_path)
    comressed_rgb_local_path = os.path.join("/fast/s3/images/rgb/", "c_" + rgb_image_name)
    rgb_im.save(comressed_rgb_local_path, "JPEG", optimize=True, quality=50)

    s3_rgb_path = os.path.join("images/rgb/", rgb_image_name)
    s3_rgb__compressed_path = os.path.join("images/rgb/compressed/", rgb_image_name)
    if not key_exists(S3_BUCKET, s3_rgb_path):
        print("Uploading %s -> %s" % (rgb_path, s3_rgb_path))
        s3.meta.client.upload_file(rgb_path, 'noaa-data', s3_rgb_path)
    if not key_exists(S3_BUCKET, s3_rgb__compressed_path):
        print("Uploading %s -> %s" % (rgb_path, s3_rgb__compressed_path))
        s3.meta.client.upload_file(comressed_rgb_local_path, 'noaa-data', s3_rgb__compressed_path)

    s3_ir_path = None
    ir_im = None
    if ir_exists:
        s3_ir_path = os.path.join("images/ir/", ir_image_name)
        if not key_exists(S3_BUCKET, s3_ir_path):
            print("Uploading %s -> %s" % (ir_path, s3_ir_path))
            s3.meta.client.upload_file(ir_path, 'noaa-data', s3_ir_path)
        ir_im = Image.open(ir_path)


    s = Session()
    # Insert image if they don't already exist in table

    rgb_db_obj=None
    if not queries.image_exists(s, rgb_image_name):
        rgb_db_obj = NOAAImage(
            file_name=rgb_image_name,
            file_path=s3_rgb__compressed_path,
            type="RGB",
            foggy=fog,
            width=rgb_im.width,
            height=rgb_im.height,
            depth=rgb_im.layers,
            bad_res= None if not status else "bad_res" in status.split("-"),
            survey=a['survey'],
            flight=a['flight'],
            cam_position=a['camPos'],
            timestamp=timestamp_obj
        )
        s.add(rgb_db_obj)
    ir_db_obj= None
    if ir_image_name is not None and not queries.image_exists(s, ir_image_name):
        ir_db_obj = NOAAImage(
            file_name=ir_image_name,
            file_path=s3_ir_path,
            type="IR",
            foggy=fog,
            width=ir_im.width,
            height=ir_im.height,
            depth=1,
            survey=a['survey'],
            flight=a['flight'],
            cam_position=a['camPos'],
            timestamp=timestamp_obj
        )
        s.add(ir_db_obj)
    rgb_db_img = get_image(s, rgb_image_name)
    ir_db_img = None if ir_image_name is None else get_image(s, ir_image_name)



    if not job_exists(s, job_name):
        j = Job(
            job_name=job_name,
            file_path= job_path,
            notes=""
        )
        s.add(j)
    j = get_job_by_name(s, job_name)
    if not worker_exists(s, worker_name):
        w = Worker(
            name=worker_name,
            human=True
        )
        s.add(w)
    w = get_worker(s, worker_name)

    if not species_exists(s, species_id):
        species_row = Species(name=species_id)
        s.add(species_row)
    sp = get_species(s,species_id)
    age_class = None if not status else status.split("-")[0]
    label_entry_ir = None
    if ir_exists:
        label_entry_ir = Label(
            image = (ir_db_img.id if ir_db_img else None),
            species = sp.id,
            x1 = thermal_x, # TODO set earlier
            x2 = thermal_x,
            y1 = thermal_y,
            y2 = thermal_y,
            age_class = age_class,
            confidence = species_confidence,
            is_shadow = pb_id is not None and pb_id[-1] == "s",
            start_date = datetime.now(),
            hotspot_id = hotspot_id,
            worker = w.id,
            job = j.id
        )
        s.add(label_entry_ir)
    label_entry_rgb = Label(
        image = (rgb_db_img.id if rgb_db_img else None),
        species = sp.id,
        x1 = x1, # TODO set earlier
        x2 = x2,
        y1 = y1,
        y2 = y2,
        age_class = age_class,
        confidence = species_confidence,
        is_shadow = pb_id is not None and pb_id[-1] == "s",
        start_date = datetime.now(),
        hotspot_id = pb_id if pb_id else hotspot_id,
        worker = w.id,
        job = j.id
    )
    s.add(label_entry_rgb)
    s.flush()

    l = Hotspot(
        eo_label = None if not rgb_exists else label_entry_rgb.id,
        ir_label = None if not ir_exists else label_entry_ir.id,
        hs_id = pb_id if pb_id else hotspot_id,
        eo_accepted = False,
        ir_accepted = False  # TODO ir x1x2etc
    )
    s.add(l)
    s.commit()
    s.close()
    x=1