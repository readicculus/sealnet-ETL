import os
from datetime import datetime
import json

import numpy as np
import pandas as pd
from PIL import Image

from noaadb import Session
from noaadb.schema.models import NOAAImage, Species, Label, FalsePositives, Hotspot
from noaadb.schema.queries import get_image, add_job_if_not_exists, add_worker_if_not_exists, get_species, \
    get_existing_label, get_existing_falsepositive, get_existing_hotspot
from noaadb.schema.schema_ops import refresh_schema
from scripts.util import parse_timestamp, printProgressBar

refresh=False
if refresh:
    refresh_schema()


transform_files = "/Downloads/viame/seal_tk/configs/pipelines/transformations/Kotz-2019-Flight-Center.h5"

directories = \
    {
        "CENT":
            {
                "/data2/2019/fl05/2019TestF5C_tinyYolo_eo_20190905_processed.csv": "/data2/2019/fl05/CENT/",
                "/data2/2019/fl04/2019TestF4C_tinyYolo_eo_20190904_processed.csv": "/data2/2019/fl04/CENT/"
            },
        "LEFT":
            {
                "/data2/2019/fl05/2019TestF5L_tinyYolo_eo_20190905_processed.csv":"/data2/2019/fl05/LEFT/",
                "/data2/2019/fl04/2019TestF4L_tinyYolo_eo_20190905_processed.csv":"/data2/2019/fl04/LEFT/"
            }
    }

def append_species(s, species_id):
    sp = get_species(s, species_id)
    if not sp:
        sp = Species(name=species_id)
        s.add(sp)
        try:
            s.flush()
        except:
            s.rollback()
            sp = get_species(s, species_id)
    return sp

def append_image(session, df_row, base_path, type):
    file_name = df_row["image"]
    db_row = get_image(session, file_name)
    if not db_row:
        image_path = os.path.join(base_path, file_name)
        name_parts= file_name.split('_')
        flight = name_parts[3]
        cam = name_parts[4]
        day = name_parts[5]
        time = name_parts[6]
        timestamp = parse_timestamp(day+time+"GMT")


        meta_name = '_'.join(name_parts[:-1])+"_meta.json"
        ir_name = '_'.join(name_parts[:-1])+"_ir.tif"
        ir_meta = None
        rgb_meta = None
        w_rgb,h_rgb = None, None
        w_ir,h_ir = None, None
        ir_exists = True
        try:
            meta = None
            with open(os.path.join(base_path, meta_name), 'r') as f:
                meta = f.read()
            meta = json.loads(meta)
            ir_meta = {i:meta[i] for i in meta if i!='rgb'}
            rgb_meta = {i:meta[i] for i in meta if i!='ir'}
            w_rgb = int(rgb_meta['rgb']['width'])
            h_rgb = int(rgb_meta['rgb']['height'])
            w_ir = int(ir_meta['ir']['width'])
            h_ir = int(ir_meta['ir']['height'])
        except:
            im = Image.open(image_path)
            w_rgb = im.width
            h_rgb = im.width
            try:
                im = Image.open(os.path.join(base_path, ir_name))
                w_ir = im.width
                h_ir = im.height
            except:
                ir_exists = False
        if ir_exists:
            ir_row = NOAAImage(
                file_name=ir_name,
                file_path=os.path.join(base_path, ir_name),
                type="IR",
                width= w_ir,
                height=h_ir,
                depth=1,
                survey='test_kotz_2019',
                flight=flight,
                cam_position=cam,
                timestamp=timestamp,
                meta=ir_meta
            )
            session.add(ir_row)
            try:
                session.flush()
            except:
                session.rollback()
                ir_row = get_image(session, ir_name)

        db_row = NOAAImage(
            file_name=file_name,
            file_path=image_path,
            type=type,
            width= w_rgb,
            height=h_rgb,
            depth=3,
            survey='test_kotz_2019',
            flight=flight,
            cam_position=cam,
            timestamp=timestamp,
            meta=rgb_meta
        )
        session.add(db_row)
        try:
            session.flush()
            # print("Inserted Image: %s" % db_row.file_name)
        except Exception as e:
            session.rollback()
            db_row = get_image(session, file_name)
    return db_row



def initialize_incorrect(s, rows, flight_detections, base_path):
    incorrect_species_id = "false positive"
    total=len(rows)
    j=0
    for i, row in rows.iterrows():
        printProgressBar(j, total, prefix='Progress:', suffix='Complete', length=50)
        j+=1
        im = append_image(s, row, base_path, "RGB")
        job = add_job_if_not_exists(s, "test_flight_kotz_2019_review", flight_detections)
        worker = add_worker_if_not_exists(s, "YOLO", False)
        species = append_species(s, incorrect_species_id)

        label_entry = Label(
            image=im,
            species=species,
            x1=row['x1'],  # TODO set earlier
            x2=row['x2'],
            y1=row['y1'],
            y2=row['y2'],
            confidence=row['confidence'],
            start_date=datetime.now(),
            end_date=datetime.now(),
            is_shadow=False,
            worker=worker,
            job=job
        )
        label_entry_exists = get_existing_label(s, label_entry)
        if not label_entry_exists:
            try:
                s.add(label_entry)
                s.flush()
            except:
                s.rollback()
                label_entry = get_existing_label(s, label_entry)
        else:
            label_entry = label_entry_exists

        fp_entry = FalsePositives(
            eo_label=label_entry,
            ir_label=None,
            hs_id=None
        )
        fp_entry_exists = get_existing_falsepositive(s, fp_entry)
        if not fp_entry_exists:
            try:
                s.add(fp_entry)
                s.flush()
            except:
                s.rollback()

        s.commit()

def initialize_correct(s, rows, flight_detections, base_path):
    species_map = {'unknown_seal': 'UNK Seal',
                   'unknown_pup': 'UNK Seal',
                   'ringed_seal': 'Ringed Seal',
                   'ringed_pup':'Ringed Seal',
                   'bearded_seal': 'Bearded Seal',
                   'bearded_pup':'Bearded Seal',
                   'animal':'animal'}

    total = len(rows)
    j=0
    for i, row in rows.iterrows():
        printProgressBar(j, total, prefix='Progress:', suffix='Complete', length=50)
        j+=1
        im = append_image(s, row, base_path, "RGB")

        species_id = "UNK" if pd.isnull(row.species) else species_map[row.species]
        is_pup = not pd.isnull(row.species) and 'pup' in row.species

        job = add_job_if_not_exists(s, "test_flight_kotz_2019_review", flight_detections)
        worker = add_worker_if_not_exists(s, "YOLO+HUMAN", True)
        species = append_species(s, species_id)

        label_entry = Label(
            image=im,
            species=species,
            x1=row['x1'],  # TODO set earlier
            x2=row['x2'],
            y1=row['y1'],
            y2=row['y2'],
            confidence=row['confidence'],
            start_date=datetime.now(),
            age_class="Pup" if is_pup else "Adult",
            end_date=None,
            is_shadow=False,
            worker=worker,
            job=job
        )
        label_entry_exists = get_existing_label(s, label_entry)
        if not label_entry_exists:
            try:
                s.add(label_entry)
                s.flush()
            except:
                s.rollback()
                label_entry = get_existing_label(s, label_entry)
        else:
            label_entry = label_entry_exists

        hs_entry = Hotspot(
            eo_label=label_entry,
            ir_label=None,
            hs_id=None,
            ir_accepted=False,
            eo_accepted=False
        )
        fp_entry_exists = get_existing_hotspot(s, hs_entry)
        if not fp_entry_exists:
            try:
                s.add(hs_entry)
                s.flush()
            except:
                s.rollback()

        s.commit()

s = Session()
# n=0
for cam in directories:
    for flight_detections, base_path in directories[cam].items():
        # if n==1: continue
        print("Working on %s" % flight_detections)
        pb_df = pd.read_csv(flight_detections, header=None)
        pb_df.columns = ['id', 'image', 'num_dets', 'x1', 'y1', 'x2', 'y2', 'confidence', 'idk',  'species','conf2']
        not_correct = pb_df.loc[pb_df['species'] == 'incorrect']
        correct = pb_df.loc[pb_df['species'] != 'incorrect']
        print(pb_df.species.unique())
        # add incorrect
        print("Correct labels (Verified)")
        initialize_correct(s, correct, flight_detections, base_path)
        print("Incorrect labels (FP)")
        initialize_incorrect(s, not_correct, flight_detections, base_path)