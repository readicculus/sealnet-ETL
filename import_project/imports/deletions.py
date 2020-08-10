import logging

import mlflow

from noaadb import Session
from noaadb.schema.models import Flight, Camera, Survey, IRImage, HeaderMeta, EOImage, IRLabelEntry, EOLabelEntry


def delete_cam_labels(s, dataset, cam, SURVEY):
    mlflow.log_param('cam', dataset.get_cam_id(cam))
    mlflow.log_param('flight', dataset.flight)
    mlflow.log_param('survey', SURVEY)
    ir_labels = s.query(IRLabelEntry).join(IRImage).join(HeaderMeta).join(Camera).filter_by(cam_name=dataset.get_cam_id(cam)) \
        .join(Flight).filter_by(flight_name=dataset.flight) \
        .join(Survey).filter_by(name=SURVEY).all()
    eo_labels = s.query(EOLabelEntry).join(EOImage).join(HeaderMeta).join(Camera).filter_by(cam_name=dataset.get_cam_id(cam)) \
        .join(Flight).filter_by(flight_name=dataset.flight) \
        .join(Survey).filter_by(name=SURVEY).all()

    num_ir_labels_pre = len(ir_labels)
    num_eo_labels_pre = len(eo_labels)

    for label in ir_labels:
        s.delete(label)
    for label in eo_labels:
        s.delete(label)
    num_ir_labels_post = s.query(IRLabelEntry).join(IRImage).join(HeaderMeta).join(Camera).filter_by(cam_name=dataset.get_cam_id(cam)) \
        .join(Flight).filter_by(flight_name=dataset.flight) \
        .join(Survey).filter_by(name=SURVEY).count()
    num_eo_labels_post = s.query(EOLabelEntry).join(EOImage).join(HeaderMeta).join(Camera).filter_by(cam_name=dataset.get_cam_id(cam)) \
        .join(Flight).filter_by(flight_name=dataset.flight) \
        .join(Survey).filter_by(name=SURVEY).count()

    s.flush()
    s.commit()
    mlflow.log_metric('ir_labels', num_ir_labels_pre - num_ir_labels_post)
    mlflow.log_metric('eo_labels', num_eo_labels_pre - num_eo_labels_post)


def delete_cam_images(dataset, cam, SURVEY):
    mlflow.log_param('cam', dataset.get_cam_id(cam))
    mlflow.log_param('flight', dataset.flight)
    mlflow.log_param('survey', SURVEY)
    tries = 0
    while tries < 5:
        tries+=1
        s = Session()
        try:
            num_ir_images_pre = s.query(IRImage).join(HeaderMeta).join(Camera).filter_by(cam_name=dataset.get_cam_id(cam))\
                .join(Flight).filter_by(flight_name=dataset.flight)\
                .join(Survey).filter_by(name=SURVEY).count()
            num_eo_images_pre = s.query(EOImage).join(HeaderMeta).join(Camera).filter_by(cam_name=dataset.get_cam_id(cam))\
                .join(Flight).filter_by(flight_name=dataset.flight)\
                .join(Survey).filter_by(name=SURVEY).count()
            num_headers_pre =  s.query(HeaderMeta).join(Camera).filter_by(cam_name=dataset.get_cam_id(cam))\
                .join(Flight).filter_by(flight_name=dataset.flight)\
                .join(Survey).filter_by(name=SURVEY).count()
            cam_obj = s.query(Camera).filter_by(cam_name=dataset.get_cam_id(cam))\
                .join(Flight).filter_by(flight_name=dataset.flight)\
                .join(Survey).filter_by(name=SURVEY).first()
            if not cam_obj:
                s.close()
                return True
            s.delete(cam_obj)

            s.commit()
            num_ir_images_post = s.query(IRImage).join(HeaderMeta).join(Camera).filter_by(cam_name=dataset.get_cam_id(cam))\
                .join(Flight).filter_by(flight_name=dataset.flight)\
                .join(Survey).filter_by(name=SURVEY).count()
            num_eo_images_post = s.query(EOImage).join(HeaderMeta).join(Camera).filter_by(cam_name=dataset.get_cam_id(cam))\
                .join(Flight).filter_by(flight_name=dataset.flight)\
                .join(Survey).filter_by(name=SURVEY).count()
            num_headers_post =  s.query(HeaderMeta).join(Camera).filter_by(cam_name=dataset.get_cam_id(cam))\
                .join(Flight).filter_by(flight_name=dataset.flight)\
                .join(Survey).filter_by(name=SURVEY).count()

            mlflow.log_metric('headers', num_headers_pre - num_headers_post)
            mlflow.log_metric('eo_images', num_eo_images_pre - num_eo_images_post)
            mlflow.log_metric('ir_images', num_ir_images_pre - num_ir_images_post)

            logging.info("DELETED")
            s.close()
            return True
        except Exception as e:
            logging.error("FAILED TO DELETE %s" %(dataset.get_cam_id(cam)))
            s.rollback()
            s.close()
            print(e)
            print("rolling back")

    return False

