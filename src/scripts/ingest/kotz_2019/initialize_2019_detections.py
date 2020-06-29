import glob
import os
from datetime import datetime

import pandas as pd
from PIL import Image
from build.lib.noaadb import Session
from noaadb.schema.models import NOAAImage, \
    Sighting, IRLabelEntry, EOLabelEntry, LabelType, ImageType
from noaadb.schema.queries import get_image, add_job_if_not_exists, add_worker_if_not_exists, get_existing_eo_label
from scripts.ingest.kotz_2019.ingest_util import append_meta, append_species
from scripts.util import parse_timestamp, printProgressBar
from sqlalchemy.exc import IntegrityError

transform_files = "/Downloads/viame/seal_tk/configs/pipelines/transformations/Kotz-2019-Flight-Center.h5"
SURVEY = 'test_kotz_2019'
JOB = 'kotz_manual_review'




class RegisteredDetections():
    def __init__(self, image_dir, eo_file, ir_file, eo_worker, ir_worker, job, name):
        self.eo_file = eo_file
        self.ir_file = ir_file
        self.image_dir = image_dir
        self.eo_worker = eo_worker
        self.ir_worker = ir_worker
        self.job = job
        self.name = name

        eo_df = pd.read_csv(self.eo_file, header=None)
        eo_df.columns = ['id', 'image', 'num_dets', 'x1', 'y1', 'x2', 'y2', 'confidence', 'idk', 'species', 'conf2']
        eo_df.columns = [str(col) + '_eo' for col in eo_df.columns]


        if self.ir_file is None:
            self.data = eo_df
            self.has_ir = False
        else:
            self.has_ir = True
            ir_df = pd.read_csv(self.ir_file, header=None)
            ir_df.columns = ['id', 'image', 'num_dets', 'x1', 'y1', 'x2', 'y2', 'conf2', 'idk', 'species', 'confidence']
            ir_df.columns = [str(col) + '_ir' for col in ir_df.columns]

            merged = pd.merge(eo_df, ir_df, left_on='id_eo', right_on='id_ir', how='left')
            merged.drop(['id_ir', 'conf2_ir', 'conf2_eo', 'species_ir', 'idk_ir', 'idk_eo', 'num_dets_ir'], axis=1, inplace=True)
            # assert(len(merged) - merged.image_ir.isnull().sum() - len(ir_df) == 0)
            if len(merged) - merged.image_ir.isnull().sum() - len(ir_df) != 0:
                print ("len(merged) - merged.image_ir.isnull().sum() - len(ir_df) = %d" % (len(merged) - merged.image_ir.isnull().sum() - len(ir_df)))
            self.data = merged
            self.data.loc[self.data['confidence_ir'] > 1, 'confidence_ir'] = 1
            int_cols_ir = ['x1_ir', 'y1_ir', 'x2_ir', 'y2_ir']
            self.data[int_cols_ir] = self.data[int_cols_ir].round()

        self.data.loc[self.data['confidence_eo'] > 1, 'confidence_eo'] = 1
        int_cols_eo = ['x1_eo', 'y1_eo', 'x2_eo', 'y2_eo']
        self.data[int_cols_eo] = self.data[int_cols_eo].round()


        self.data.rename(columns={'species_eo': 'species', 'id_eo': 'id'}, inplace=True)
        not_correct = self.data.loc[self.data ['species'] == 'incorrect']
        self.verified_incorrect = not_correct.loc[not_correct['confidence_eo'] >= .5]
        self.verified_correct = self.data.loc[self.data['species'] != 'incorrect']

        self.job = None
        self.eo_worker = None
        self.ir_worker = None

    def print_info(self):
        print(self.name)
        print("%d detections correct" % len(self.verified_correct))
        print("%d detections incorrect" % len(self.verified_incorrect))
        print("unique_species: %s" % self.verified_correct.species.unique())
        print("Has IR:  " + str(self.has_ir))
        print()

    def create_job(self, s):
        self.job = add_job_if_not_exists(s, "test_flight_kotz_2019_review", self.eo_file)
        self.eo_worker = add_worker_if_not_exists(s, "Yolo/Gavin", True)
        if self.has_ir:
            self.ir_worker = add_worker_if_not_exists(s, "Projected", False)

    # def process_images(self, s):
    #     rgb_images = glob.glob(os.path.join(self.image_dir, '*_rgb.jpg'))
    #     append_images(s, rgb_images)

    def process_incorrect(self, s):
        process_labels(s, self.verified_incorrect, self.job, self.eo_worker, self.ir_worker, LabelType.FP)

    def process_correct(self, s):
        process_labels(s, self.verified_correct, self.job, self.eo_worker, self.ir_worker, LabelType.TP)

fl04_C = RegisteredDetections('/data2/2019/fl04/CENT/',
                     '/data2/2019/fl04/2019TestF4C_tinyYolo_eo_20190904_processed.csv',
                     '/data2/2019/fl04/2019TestF4C_tinyYolo_ir_20190904_projected.csv',
                     'Yolo/Gavin',
                     'output_transform_4Center.h5',
                              JOB, 'fl04_C')

# fl05_C = RegisteredDetections('/data2/2019/fl05/CENT/',
#                      '/data2/2019/fl05/2019TestF5C_tinyYolo_eo_20190905_processed.csv',
#                      '/data2/2019/fl05/2019TestF5C_tinyYolo_ir_20190905_projected.csv',
#                      'Yolo/Gavin',
#                      'output_transform_4Center.h5',
#                               JOB, 'fl05_C')
# #
# fl01_C = RegisteredDetections('/data2/2019/fl01/CENT/',
#                      '/data2/2019/fl01/2019TestF1C_tinyYolo_eo_20190813_processed.csv',
#                      None,
#                      'Yolo/Gavin',
#                      None,
#                               JOB, 'fl01_C')

# Notes: ir seems really bad and maybe alignment for some reason..?
# fl04_L = RegisteredDetections('/data2/2019/fl04/LEFT/',
#                      '/data2/2019/fl04/2019TestF4L_tinyYolo_eo_20190905_processed.csv',
#                      None,
#                      'Yolo/Gavin',
#                      None,
#                               JOB, 'fl04_L')
#
# fl05_L = RegisteredDetections('/data2/2019/fl05/LEFT/',
#                      '/data2/2019/fl05/2019TestF5L_tinyYolo_eo_20190905_processed.csv',
#                      '/data2/2019/fl05/2019TestF5L_tinyYolo_ir_20190905_projected.csv',
#                      'Yolo/Gavin',
#                      'output_transform_Left.h5',
#                               JOB, 'fl05_L')

# registered_list = [fl01_C, fl04_C, fl04_L, fl05_C, fl05_L]
# registered_list = [fl01_C, fl04_C, fl04_L, fl05_C, fl05_L]
registered_list = [fl04_C]



def append_images(session, images):
    j = 0
    total = len(images)
    for image_path in images:
        printProgressBar(j, total, prefix='Progress:', suffix='Complete', length=50)
        j += 1
        base_path, file_name = os.path.dirname(image_path), os.path.basename(image_path)
        db_row = get_image(session, file_name)

        if not db_row:
            name_parts= file_name.split('_')
            start_idx = 3
            # fl01 images have slightly  different names
            if name_parts[2] != '2019':
                start_idx = 2
            flight = name_parts[start_idx]
            cam = name_parts[start_idx+1]
            day = name_parts[start_idx+2]
            time = name_parts[start_idx+3]
            timestamp = parse_timestamp(day+time+"GMT")


            meta_name = '_'.join(name_parts[:-1])+"_meta.json"
            ir_name = '_'.join(name_parts[:-1])+"_ir.tif"
            w_ir,h_ir = None, None
            ir_exists = True
            try:
                im = Image.open(os.path.join(base_path, ir_name))
                w_ir = im.width
                h_ir = im.height
            except:
                ir_exists = False

            rgb_obj, ir_obj = append_meta(session, os.path.join(base_path, meta_name))
            if ir_exists:
                ir_row = NOAAImage(
                    file_name=ir_name,
                    file_path=os.path.join(base_path, ir_name),
                    type=ImageType.IR,
                    width= w_ir,
                    height=h_ir,
                    depth=1,
                    survey=SURVEY,
                    flight=flight,
                    cam_position=cam,
                    timestamp=timestamp,
                    flight_meta=ir_obj,
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
                type=ImageType.RGB,
                width= im.width,
                height=im.height,
                depth=3,
                survey=SURVEY,
                flight=flight,
                cam_position=cam,
                timestamp=timestamp,
                flight_meta=rgb_obj
            )
            session.add(db_row)
            try:
                session.flush()
                # print("Inserted Image: %s" % db_row.file_name)
            except Exception as e:
                session.rollback()
                db_row = get_image(session, file_name)

        if j % 100 == 0:
            session.commit()
    session.commit()


def add_label(s, row, im, worker, job, species, type):
    iseo = (type=='eo')
    disc = ImageType.RGB if iseo else ImageType.IR
    LabelClass = EOLabelEntry if iseo else IRLabelEntry
    label_entry = LabelClass(
        image=im,
        x1=row['x1_eo'] if iseo else row['x1_ir'],
        x2=row['x2_eo'] if iseo else row['x2_ir'],
        y1=row['y1_eo'] if iseo else row['y1_ir'],
        y2=row['y2_eo'] if iseo else row['y2_ir'],
        species=species,
        discriminator=disc
    )
    check = get_existing_eo_label(s, label_entry)
    if check is not None:
        return check
    label_entry = LabelClass(
        image=im,
        x1=row['x1_eo'] if iseo else row['x1_ir'],  # TODO set earlier
        x2=row['x2_eo'] if iseo else row['x2_ir'],
        y1=row['y1_eo'] if iseo else row['y1_ir'],
        y2=row['y2_eo'] if iseo else row['y2_ir'],
        confidence=row['confidence_eo'],
        start_date=datetime.now(),
        end_date=None,
        is_shadow=None,
        worker=worker,
        job=job,
        discriminator=disc,
        species=species
    )
    s.add(label_entry)
    try:
        s.flush()
    except IntegrityError as e:
        s.rollback()
        return None

    return label_entry

def process_labels(s, rows, job, eo_worker, ir_worker, disc):
    species_map = {'unknown_seal': 'UNK Seal',
                   'unknown_pup': 'UNK Seal',
                   'ringed_seal': 'Ringed Seal',
                   'ringed_pup':'Ringed Seal',
                   'bearded_seal': 'Bearded Seal',
                   'bearded_pup':'Bearded Seal',
                   'animal':'animal',
                   'Ringed Seal':'Ringed Seal',
                   'Bearded Seal': 'Bearded Seal',
                   'Polar Bear': 'Polar Bear',
                   'incorrect': 'falsepositive'}

    total = len(rows)
    num_ir_missing= 0
    j= 0
    for i, row in rows.iterrows():
        if j % 10 == 0:
            printProgressBar(j, total, prefix='Progress:', suffix='Complete', length=50)
        j+=1
        species_id = "UNK" if pd.isnull(row.species) else species_map[row.species]
        is_pup = not pd.isnull(row.species) and 'pup' in row.species
        age_class = "Pup" if is_pup else "Adult"
        if species_id == 'falsepositive':
            age_class = None
        species = append_species(s, species_id)

        im_eo = get_image(s, row["image_eo"])
        label_entry_eo, label_entry_ir = None, None
        label_entry_eo = add_label(s, row, im_eo, eo_worker, job, species,'eo')

        # sighting.labels.extend([label_entry_eo])
        if im_eo is None:
            print("ERROR %s" % os.path.basename(row["image_eo"]))
        if ir_worker:
            if pd.isna(row["image_ir"]):  # no ir match for this image
                num_ir_missing += 1
            else:
                im_ir = get_image(s, os.path.basename(row["image_ir"]))
                if im_ir is None:
                    print("ERROR %s" % os.path.basename(row["image_ir"]))
                else:
                    label_entry_ir = add_label(s, row, im_ir, ir_worker, job, species, 'ir')

                    # sighting.labels.extend([label_entry_ir])

        sighting = Sighting(
            age_class=age_class,
            species=species,
            discriminator=disc,
            eo_label=label_entry_eo,
            ir_label=label_entry_ir
        )
        s.add(sighting)
        try:
            s.flush()
        except IntegrityError as e:
            print(e)
            s.rollback()

        if j % 100 == 0:
            printProgressBar(j, total, prefix='Progress:', suffix='Committing', length=50)
            s.commit()
            s.flush()
    s.commit()
    s.flush()
    print("%d images have no IR/not aligned" % num_ir_missing)




def add_all():
    # n=0
    for registered_pair in registered_list:
        registered_pair.print_info()
        registered_pair.create_job(s)
        # print("Processing all images")
        # registered_pair.process_images(s)
        print("Correct labels (Verified)")
        registered_pair.process_correct(s)
        print("Incorrect labels (FP)")
        registered_pair.process_incorrect(s)

s = Session()
# s.query(LabelEntry).all()
# remove_sightings_by_survey(s, survey=SURVEY)
# remove_labels_by_survey(s, survey=SURVEY)
add_all()
s.close()
# fix_images()
# surveys = ["CHESS-russia" , "CHESS2016", "BEAUFORT-2019"]
# for survey in surveys:
#     remove_sightings_by_survey(s, survey=survey)
# for survey in surveys:
#     remove_labels_by_survey(s, survey=survey)