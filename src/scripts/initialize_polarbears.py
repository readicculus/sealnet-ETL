import boto3
from PIL import Image
from noaadb.schema.models import NOAAImage, TruePositiveLabels, Species, Sighting, LabelEntry
from noaadb.schema.queries import species_exists, get_image, get_species, add_job_if_not_exists, \
    add_worker_if_not_exists, image_exists
from noaadb.schema.schema_ops import refresh_schema
from scripts.util import *
from dateutil import parser

from scripts.util import file_exists, parse_chess_filename
refresh = False
if refresh:
    refresh_schema()
NOAA_WORKER = "noaa"
NOAA_JOB = "noaa_original_labels"
YUVAL_WORKER = "yuval"
YUVAL_JOB = "yuvals_2019_polarbear_updates"
YUVAL_NEW_JOB = "yuvals_new_labels"

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

job_name = "polarbear_labels_v1.0"
job_path= "jobs/original_polarbear_labels.csv"

s = Session()
for i, row in pb_df.iterrows():
    vals = list(row)
    id = vals[0]
    rgb_image_name = vals[1]
    ir_image_name = None if pd.isna(vals[2]) else vals[2]
    hotspot_id = None if pd.isna(vals[3]) else int(vals[3])
    pb_id = None if pd.isna(vals[4]) else vals[4]
    hotspot_type = vals[5]
    species_id = vals[6]
    species_confidence = None if pd.isna(vals[7]) else float(vals[7].replace("%", ""))/100.
    fog = vals[8]
    thermal_x = None if pd.isna(vals[9]) else vals[9]
    thermal_y = None if pd.isna(vals[10]) else vals[10]
    updated = vals[19]
    status = None if pd.isna(vals[20]) else vals[20].replace("none", "")
    x1 = vals[15] if updated or not vals[11] else vals[11]
    y1 = vals[16] if updated or not vals[12] else vals[12]
    x2 = vals[17] if updated or not vals[13] else vals[13]
    y2 = vals[18] if updated or not vals[14] else vals[14]

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
        a['survey'] = "BEAUFORT-2019"
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
        a['survey'] = "CHESS-russia"
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
    # comressed_rgb_local_path = os.path.join("/fast/s3/images/rgb/", "c_" + rgb_image_name)
    # rgb_im.save(comressed_rgb_local_path, "JPEG", optimize=True, quality=50)

    # s3_rgb_path = os.path.join("images/rgb/", rgb_image_name)
    # s3_rgb__compressed_path = os.path.join("images/rgb/compressed/", rgb_image_name)
    # upoload_s3(s3, s3_client, 'noaa-data', rgb_path, s3_rgb_path)
    # upoload_s3(s3, s3_client, 'noaa-data',comressed_rgb_local_path, s3_rgb__compressed_path)

    # s3_ir_path = None if ir_image_name is None else os.path.join("images/ir/", ir_image_name)
    ir_im = None
    if ir_exists:
        # upoload_s3(s3, s3_client, 'noaa-data', ir_path, s3_ir_path)
        ir_im = Image.open(ir_path)

    # Insert image if they don't already exist in table
    image_quality = None

    if status is not None and is_bad_res(status):
        image_quality = 0

    rgb_db_obj=None
    if not image_exists(s, rgb_image_name):
        rgb_db_obj = NOAAImage(
            file_name=rgb_image_name,
            file_path=rgb_path,
            type="RGB",
            width=rgb_im.width,
            height=rgb_im.height,
            depth=rgb_im.layers,
            survey=a['survey'],
            flight=a['flight'],
            cam_position=a['camPos'],
            quality=image_quality,
            foggy=fog,
            timestamp=timestamp_obj
        )
        s.add(rgb_db_obj)
    ir_db_obj= None
    if ir_image_name is not None and not image_exists(s, ir_image_name):
        ir_db_obj = NOAAImage(
            file_name=ir_image_name,
            file_path=ir_path,
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

    rgb_worker_name = YUVAL_WORKER if updated else NOAA_WORKER
    rgb_job_name = YUVAL_JOB if updated else NOAA_JOB
    ir_worker_name = NOAA_WORKER
    ir_job_name = NOAA_JOB
    rgb_job = add_job_if_not_exists(s, rgb_job_name, job_path)
    rgb_worker = add_worker_if_not_exists(s, rgb_worker_name, True)

    ir_job = add_job_if_not_exists(s, ir_job_name, job_path)
    ir_worker = add_worker_if_not_exists(s, ir_worker_name, True)

    if not species_exists(s, species_id):
        species_row = Species(name=species_id)
        s.add(species_row)
    sp = get_species(s,species_id)
    age_class = None if not status else status.split("-")[0]
    label_entry_ir = None
    to_add = []
    if ir_exists and thermal_x is not None and thermal_y is not None:
        label_entry_ir = TruePositiveLabels(
            image = ir_db_img,
            x1 = thermal_x, # TODO set earlier
            x2 = thermal_x,
            y1 = thermal_y,
            y2 = thermal_y,
            confidence = species_confidence,
            is_shadow = pb_id is not None and pb_id[-1] == "s",
            start_date = datetime.now(),
            worker = ir_worker,
            job = ir_job
        )
        to_add.append(label_entry_ir)
    label_entry_rgb = LabelEntry(
        image = rgb_db_img,
        x1 = x1, # TODO set earlier
        x2 = x2,
        y1 = y1,
        y2 = y2,
        confidence = species_confidence,
        is_shadow = pb_id is not None and pb_id[-1] == "s",
        start_date = datetime.now(),
        worker = rgb_worker,
        job = rgb_job
    )
    to_add.append(label_entry_rgb)

    l = Sighting(
        hotspot_id=pb_id if pb_id else hotspot_id,
        age_class=age_class,
        species=sp
    )
    to_add.append(l)
    s.add_all(to_add)
    s.commit()
    s.close()
    x=1