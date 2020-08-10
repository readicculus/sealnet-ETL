# Convert CHESS to standard format
import boto3
from PIL import Image

from build.lib.noaadb import Session
from import_project.imports import experiment

from import_project.utils.util import *
refresh = False
upload_s3 = False
log_existing = True


NOAA_WORKER = "noaa"
NOAA_JOB = "noaa_original_labels"
YUVAL_WORKER = "yuval"
YUVAL_JOB = "yuvals_2019_relabel_mission"
YUVAL_NEW_JOB = "yuvals_new_labels"


LOCAL_S3 = "/data/raw_data/PolarBears/s3_images/"
s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
S3_BUCKET = "noaa-data"

# pb_df = pd.read_csv("updated_seals.csv")
# job_path= "jobs/updated_seals.csv"
# new labels RGB 6344 IR 5970
# new hotspots 6344
# new images RGB 4882 IR 4907

pb_df = pd.read_csv("/home/yuval/Documents/XNOR/sealnet-ETL/import_project/data/TrainingAnimals_WithSightings_updating.csv")



job_path= "jobs/TrainingAnimals_WithSightings_updating.csv"
# new labels RGB 955 IR 957
# new hotspots 489
# new images RGB 285 IR 683

rgb_dirs = ["/data/raw_data/TrainingAnimals_ColorImages"]
ir_dirs = ["/data/raw_data/TrainingAnimals_ThermalImages", "/data/raw_data/ALL_THERMAL"]
session = Session()

total = len(pb_df)
# returns None if pd.isna(val) otherwise returns val
def none_if_na(val): return None if pd.isna(val) else val
def is_removed(status): return "removed" in status
def is_new_label(status): return "new" in status
def is_bad_res(status): return "bad_res" in status
def is_maybe_seal(status): return "maybe_seal" in status
def is_off_edge(status): return "off_edge" in status
def find_image(dirs, name):
    for dir in dirs:
        im_path = os.path.join(dir, name).strip()
        if os.path.exists(im_path):
            return im_path
    return None

rgb_rows_list = []
ir_rows_list = []
with mlflow.start_run(run_name='standardize_chess_csv', experiment_id=experiment.experiment_id):
    for i, row in pb_df.iterrows():
        vals = list(row)
        id = vals[0]
        rgb_image_name = vals[1]
        ir_image_name = none_if_na(vals[2])
        hotspot_type = vals[4]
        species_id = vals[5]

        fog = None if pd.isna(vals[7]) else vals[7]
        if fog == "No":
            fog = False
        elif fog == "Yes":
            fog = True
        else:
            fog = None

        thermal_x = None if pd.isna(vals[8]) else vals[8]
        thermal_y = None if pd.isna(vals[9]) else vals[9]

        status = None if pd.isna(vals[19]) else vals[19].replace("none", "")
        updated = vals[18]
        removed = is_removed(status)
        bas_res = is_bad_res(status)
        is_new = is_new_label(status)

        x1 = vals[14] if updated and not removed else vals[10]
        y1 = vals[15] if updated and not removed else vals[11]
        x2 = vals[16] if updated and not removed else vals[12]
        y2 = vals[17] if updated and not removed else vals[13]

        species_confidence = None if pd.isna(vals[6]) else vals[6]
        if is_new:
            # species_confidence = .3
            pass
        elif species_confidence == "No":
            # species_confidence = 0
            pass
        elif species_confidence == "Likely":
            # species_confidence = .9
            pass
        elif species_confidence == "Guess":
            # species_confidence = .6
            pass
        elif species_confidence is not None:
            species_confidence = int(species_confidence.replace("%", ""))/100.0

        image_quality = None
        if is_bad_res(status):
            image_quality = 0

        if is_maybe_seal(status):
            species_confidence = .3  # same as "Guess"
        off_edge = is_off_edge(status)

        rgb_file_info = {}
        timestamp_obj = None

        rgb_path = find_image(rgb_dirs, rgb_image_name)
        ir_path = None if ir_image_name is None else find_image(ir_dirs, ir_image_name)

        hotspot_id = None if (pd.isna(vals[3]) or is_new) else str(vals[3])
        if rgb_path is None:
            print("Not found RGB!", rgb_image_name)
        if ir_image_name is not None and ir_path is None:
            print("Not found IR!", ir_image_name)

        if removed or bas_res:
            continue

        if rgb_path:
            rgb_rows_list.append([i, rgb_path, x1, x2, y1, y2, species_confidence, species_id, hotspot_id])
        if ir_path:
            ir_rows_list.append([i, ir_path, thermal_x, thermal_x, thermal_y, thermal_y, species_confidence, species_id, hotspot_id])


    color_df_out = pd.DataFrame(rgb_rows_list,
                                columns=['id', 'image', 'x1', 'x2', 'y1', 'y2', 'confidence', 'species', 'hs_id'])
    ir_df_out = pd.DataFrame(ir_rows_list,
                                columns=['id', 'image', 'x1', 'x2', 'y1', 'y2', 'confidence', 'species', 'hs_id'])
    x=1
    ir_csv_path = "/home/yuval/Documents/XNOR/sealnet-ETL/import_project/data/TrainingAnimals_WithSightings_updating_standardized_ir.csv"
    eo_csv_path = "/home/yuval/Documents/XNOR/sealnet-ETL/import_project/data/TrainingAnimals_WithSightings_updating_standardized_eo.csv"
    ir_df_out.to_csv(ir_csv_path, index=False)
    color_df_out.to_csv(eo_csv_path, index=False)
    mlflow.log_artifact(ir_csv_path)
    mlflow.log_artifact(eo_csv_path)
