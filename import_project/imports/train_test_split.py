import random

import mlflow
from sqlalchemy import create_engine, DDL

from import_project.imports import experiment
from noaadb import DATABASE_URI, Session
from noaadb.schema.models import EOIRLabelPair, IRLabelEntry, EOLabelEntry, Species
from noaadb.schema.models.ml_data import TrainTestSplit, MLType
from noaadb.schema.utils.schema_ops import create_ml_schema, drop_ml_schema

from import_project.utils.util import save_list_artifact

engine = create_engine(DATABASE_URI, echo=False)
drop_ml_schema(engine, tables_only=False)
create_ml_schema(engine, tables_only=False)

with mlflow.start_run(run_name='set_train_test_split', experiment_id=experiment.experiment_id):
    s = Session()
    SPLIT = .8
    pairs = s.query(EOIRLabelPair).all()
    by_species = {}


    total_species_count = {}
    image_label_dict = {}

    for idx, r in enumerate(pairs):
        # calculate total species counts
        sp_id = r.eo_label.species_id
        if not sp_id in total_species_count:
            total_species_count[sp_id] = 0
        total_species_count[sp_id] += 1

        # organize by image id
        label_im_id = r.eo_label.image_id
        if not label_im_id in image_label_dict:
            image_label_dict[label_im_id] = []
        image_label_dict[label_im_id].append(r)

    target = {x:total_species_count[x]*SPLIT for x in total_species_count.keys()}
    train_species_count = {x:0 for x in total_species_count.keys()}
    test_species_count = {x:0 for x in total_species_count.keys()}
    # train_ids = []
    # test_ids = []
    train_images = []
    test_images = []

    im_keys = list(image_label_dict.keys())
    random.shuffle(im_keys)
    for idx, im_id in enumerate(im_keys):
        labels = image_label_dict[im_id]
        for label in labels:
            train_species_count[label.eo_label.species_id] += 1
        train_images.append(im_id)
        done=0
        for k in train_species_count:
            if train_species_count[k] > target[k]:
                done += 1


        if done >= len(target.keys()) -2:
            test_images = im_keys[idx+1:]
            break

    for idx, im_id in enumerate(test_images):
        labels = image_label_dict[im_id]
        for label in labels:
            test_species_count[label.eo_label.species_id] += 1

    save_list_artifact(test_images, 'test_images.txt', '')
    save_list_artifact(train_images, 'train_images.txt', '')
    for k in train_species_count:
        sp_obj = s.query(Species).filter(Species.id == k).first()
        mlflow.log_metric('%s_train'%sp_obj.name,train_species_count[k])
        mlflow.log_metric('%s_test'%sp_obj.name,test_species_count[k])
        mlflow.log_metric('%s_split-ratio'%sp_obj.name,train_species_count[k]/total_species_count[k])
        print("%d %d/%d=%.4f Train" % (k, train_species_count[k], total_species_count[k], train_species_count[k]/total_species_count[k]))
    s.close()
    s = Session()
    for idx, id in enumerate(train_images):
        tts = TrainTestSplit(
            image_id=id,
            type=MLType.TRAIN
        )
        s.add(tts)

    for idx, id in enumerate(test_images):
        tts = TrainTestSplit(
            image_id=id,
            type=MLType.TEST
        )
        s.add(tts)
    s.commit()
    s.close()