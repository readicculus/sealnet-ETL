# Go through noaadb image set look for pairs, register, save to s3, log in db
import os

import numpy as np
import boto3
import cv2
from sqlalchemy import create_engine
from sqlalchemy.orm import aliased
import mlflow

from import_project.imports import experiment
from noaadb import Session, DATABASE_URI
from noaadb.schema.models import EOIRLabelPair, EOImage, EOLabelEntry, IRLabelEntry, IRImage, \
    Homography
from noaadb.schema.models.survey_data import FusedImage
from import_project.imports.ModailtyTransform import ModalityTransform, TransformMode
from import_project.utils.s3_util import check

# ''' set up tracking uri '''


noaa_sess = boto3.session.Session(profile_name='default')
s3_client = noaa_sess.client('s3')
bucket_name = 'yboss'
fused_image_registry_path = 'fused_image_registry/'

bucket_loc = s3_client.get_bucket_location(Bucket=bucket_name)['LocationConstraint']

# === helper functions section ===

# get shared part of image file names for eo/ir (without the _ir.tif or _rgb.jpg)
def id_from_image_name(image_name):
    return '_'.join((os.path.splitext(image_name)[0]).split('_')[:-1])


# === main code section ===
engine = create_engine(DATABASE_URI)
# FusedImage.__table__.drop(bind = engine, checkfirst=True)
FusedImage.__table__.create(bind = engine, checkfirst=True)

# # Create our folder on s3 if not exists
if not check(s3_client, bucket_name, fused_image_registry_path):
    response = s3_client.put_object(Bucket=bucket_name, Key=fused_image_registry_path)
    print("Response: {}".format(response))  # See result of request.

# create registry
s = Session()

# query all labels in pairs
# for now we will only register images with labels
eo_label = aliased(EOLabelEntry)
ir_label = aliased(IRLabelEntry)
eo_im = aliased(EOImage)
ir_im = aliased(IRImage)
label_pairs = s.query(EOIRLabelPair, eo_im, ir_im) \
    .join(eo_label, EOIRLabelPair.eo_label) \
    .join(ir_label, EOIRLabelPair.ir_label)\
    .join(eo_im, eo_label.image)\
    .join(ir_im, ir_label.image)\
    .all()

# since labels->im is one many to one we want to just get the unique image pairs that have labels
pair_dict = {}
for _, eo_im, ir_im in label_pairs:
    shared_name = id_from_image_name(eo_im.file_name)
    if not shared_name in pair_dict:
        pair_dict[shared_name] = {'eo': eo_im, 'ir': ir_im}

# go through images, check if exists already, if not fuse and add
items = pair_dict.items()
total_items = len(items)

with mlflow.start_run(run_name='fuse_images', experiment_id=experiment.experiment_id):
    existing_images = 0
    new_images = 0
    num_added_to_db = 0
    saved_file_size = 0
    for idx, (shared_name, ims) in enumerate(items):
        print('%d/%d'%(idx,total_items))
        fused_fn = '%s_fused.png' % shared_name
        fused_im_key = os.path.join(fused_image_registry_path,fused_fn)
        local_path = os.path.join('/data/', fused_im_key)
        s3_object_url = "https://s3-{0}.amazonaws.com/{1}/{2}".format(bucket_loc, bucket_name, fused_im_key)

        local_exists = os.path.exists(local_path)
        # s3_exists = check(s3_client, bucket_name, fused_im_key)
        db_exists = s.query(FusedImage).filter(FusedImage.file_name == fused_fn).first()

        if local_exists and db_exists:
            saved_file_size += os.stat(local_path).st_size
            existing_images += 1
            continue

        # get homography
        header = ims['eo'].header_meta
        homography = s.query(Homography).filter(Homography.camera_id == header.camera_id).first()
        H = homography.matrix

        if not local_exists:
            new_images += 1
            # load images
            im_eo = cv2.imread(ims['eo'].file_path)
            im_ir = cv2.imread(ims['ir'].file_path, cv2.IMREAD_ANYDEPTH)

            # normalize IR
            im_ir = ((im_ir - np.min(im_ir)) / (0.0 + np.max(im_ir) - np.min(im_ir))) * 256.0
            im_ir = im_ir.astype(np.uint8)
            # transform
            mt_left = ModalityTransform(H, TransformMode.IRTOEO)
            ir_to_eo = mt_left.transform_ir_to_eo(im_ir, im_eo)
            h,w,c = ir_to_eo.shape
            if not os.path.exists(local_path):
                cv2.imwrite(local_path, ir_to_eo)
            saved_file_size += os.stat(local_path).st_size
        elif db_exists:
            h,w,c = db_exists.height, db_exists.width, db_exists.depth
        else:
            existing_images+=1
            im_fused = cv2.imread(local_path)
            h,w,c = im_fused.shape

        fused_obj = FusedImage(
            file_name=fused_fn,
            s3_uri=s3_object_url,
            file_path=local_path,
            eo_image_id=ims['eo'].file_name,
            ir_image_id=ims['ir'].file_name,
            homography_id=homography.id,
            width=w,
            height=h,
            depth=c,
        )
        if not db_exists:
            s.add(fused_obj)
            num_added_to_db+=1
        else:
            s.merge(fused_obj)
        s.commit()
        mlflow.log_metric('fused_image_exists', existing_images)
        mlflow.log_metric('fused_image_added', new_images)
        mlflow.log_metric('fused_dbentry_added', num_added_to_db)
        mlflow.log_metric('size_on_disk_Mb', int(saved_file_size/1000000))
        print('Added %s %s' % (local_path, s3_object_url))
        x=1
s.close()
x=1