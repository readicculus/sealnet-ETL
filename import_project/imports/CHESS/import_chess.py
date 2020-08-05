import os

import pandas as pd

# P = port = LEFT
# S = starboard = RIGHT
from import_project.utils.ingest_util import append_meta

pb_df = pd.read_csv("/import_project/data/TrainingAnimals_WithSightings_updating.csv")
job_path= "jobs/TrainingAnimals_WithSightings_updating.csv"

# append_meta()
rgb_dirs = ["/data/raw_data/TrainingAnimals_ColorImages"]
ir_dirs = ["/data/raw_data/TrainingAnimals_ThermalImages", "/data/raw_data/ALL_THERMAL"]

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

def parse_row(row):
    vals = list(row)
    id = vals[0]
    rgb_image_name = vals[1]
    ir_image_name = none_if_na(vals[2])
    pb_id = none_if_na(vals[3])
    hotspot_type = vals[4]
    species_id = vals[5]

    if species_id == "Polar Bear":
        return None

    fog = None if pd.isna(vals[7]) else vals[7]
    if fog == "No":
        fog = False
    elif fog =="Yes":
        fog = True
    else:
        fog = None

    thermal_x = None if pd.isna(vals[8]) else vals[8]
    thermal_y = None if pd.isna(vals[9]) else vals[9]

    status = None if pd.isna(vals[19]) else vals[19].replace("none", "")
    updated = vals[18]
    removed = is_removed(status)
    is_new = is_new_label(status)

    x1 = vals[14] if updated and not removed else vals[10]
    y1 = vals[15] if updated and not removed else vals[11]
    x2 = vals[16] if updated and not removed else vals[12]
    y2 = vals[17] if updated and not removed else vals[13]

    species_confidence = None if pd.isna(vals[6]) else vals[6]
    if is_new:
        species_confidence = .3
    elif species_confidence == "No":
        species_confidence = 0
    elif species_confidence == "Likely":
        species_confidence = .9
    elif species_confidence == "Guess":
        species_confidence = .6
    elif species_confidence is not None:
        species_confidence = int(species_confidence.replace("%", ""))


    image_quality = None
    if is_bad_res(status):
        image_quality = 0

    if is_maybe_seal(status):
        species_confidence = .3 # same as "Guess"
    off_edge = is_off_edge(status)

    rgb_file_info = {}
    timestamp_obj = None

    rgb_path = find_image(rgb_dirs, rgb_image_name)
    ir_path = None if ir_image_name is None else find_image(ir_dirs, ir_image_name)

    hotspot_id = None if (pd.isna(vals[3]) or is_new) else int(vals[3])

    if rgb_path is None:
        print("Not found RGB!", rgb_image_name)
    if ir_image_name is not None and ir_path is None:
        print("Not found IR!", ir_image_name)


for i, row in pb_df.iterrows():
    parse_row(row)



    x=1