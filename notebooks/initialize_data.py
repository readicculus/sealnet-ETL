import boto3
import botocore
import pandas as pd
import os
import numpy as np
from PIL import Image
from botocore.exceptions import ClientError
from sqlalchemy import create_engine

from noaadb import DATABASE_URI, Session, queries
from noaadb.models import Images



LOCAL_S3 = "/data/raw_data/PolarBears/s3_images/"
s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
S3_BUCKET = "noaa-data"

pb_df = pd.read_csv("polarbears.csv")
CHESS_PATH = "/data/raw_data/Polar_Bear/2016_Chukchi_CHESS_US/"
BEAUFORT_PATH = "/data/raw_data/Polar_Bear/2019_Beaufort_PolarBears/"
RU_PATH = "/data/raw_data/Polar_Bear/2016_Chukchi_CHESS_Russia/"
BACKUP_PATH = "/data/raw_data/TrainingAnimals_ColorImages"
BACKUP_PATH_IR = "/data/raw_data/TrainingAnimals_ThermalImages"
def file_exists(path):
    if not pd.isna(ir_image_name):
        return os.path.exists(path)
    return None

def key_exists(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return False
    return True

for i, row in pb_df.iterrows():
    vals = list(row)
    id = vals[0]
    rgb_image_name = vals[1]
    ir_image_name = vals[2]
    hotspot_id = vals[3]
    pb_id = vals[4]
    hotspot_type = vals[5]
    species_id = vals[6]
    species_confidence = vals[7]
    fog = vals[8]
    thermal_x = vals[9]
    thermal_y = vals[10]
    l = vals[11]
    t = vals[12]
    r = vals[13]
    b = vals[14]
    l_u = vals[15]
    t_u = vals[16]
    r_u = vals[17]
    b_u = vals[18]
    updated = vals[19]
    status = vals[20]

    if fog == "No":
        fog = False
    elif fog =="Yes":
        fog = True
    else:
        fog = None

    if i < 36:
        # CHESS Rows
        path = CHESS_PATH
    elif i < 222:
        # Beaufort Rows
        path = BEAUFORT_PATH
    else:
        # Russia rows
        path = RU_PATH
    rgb_path = os.path.join(path, rgb_image_name).strip()
    ir_path = os.path.join(path, ir_image_name).strip()
    rgb_exists = file_exists(rgb_path)
    ir_exists = file_exists(ir_path)
    if not rgb_exists:
        rgb_path = os.path.join(BACKUP_PATH, rgb_image_name).strip()
        rgb_exists = file_exists(rgb_path)
    if not ir_exists:
        ir_path = os.path.join(BACKUP_PATH_IR, ir_image_name).strip()
        ir_exists = file_exists(ir_path)
    if not rgb_exists:
        print("Not found!", rgb_image_name)
    if not ir_exists:
        print("Not found!", ir_image_name)


    s3_rgb_path = os.path.join("images/rgb/", rgb_image_name)
    s3_ir_path = os.path.join("images/ir/", ir_image_name)

    if not key_exists(S3_BUCKET, s3_rgb_path):
        print("Uploading %s -> %s" % (rgb_path, s3_rgb_path))
        s3.meta.client.upload_file(rgb_path, 'noaa-data', s3_rgb_path)
    if not key_exists(S3_BUCKET, s3_ir_path):
        print("Uploading %s -> %s" % (ir_path, s3_ir_path))
        s3.meta.client.upload_file(ir_path, 'noaa-data', s3_ir_path)

    rgb_im =  Image.open(rgb_path)
    ir_im =  Image.open(ir_path)
    s = Session()
    # Insert image if they don't already exist in table



    if not queries.image_exists(s, rgb_image_name):
        rgb_db_obj = Images(
            file_name=rgb_image_name,
            file_path=s3_rgb_path,
            type="RGB",
            foggy=fog,
            width=rgb_im.width,
            height=rgb_im.height,
            depth=rgb_im.layers,
        )
        s.add(rgb_db_obj)

    if not queries.image_exists(s, ir_image_name):
        ir_db_obj = Images(
            file_name=ir_image_name,
            file_path=s3_ir_path,
            type="RGB",
            foggy=fog,
            width=ir_im.width,
            height=ir_im.height,
            depth=1,
        )
        s.add(ir_db_obj)

    s.commit()
    s.close()
    x=1