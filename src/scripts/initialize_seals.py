import boto3
from PIL import Image

from build.lib.noaadb import Session
from noaadb.schema.schema_ops import refresh_schema
from noaadb.schema.models import NOAAImage, Species, Sighting, IRLabelEntry, EOLabelEntry, \
    LabelType, ImageType
from noaadb.schema.queries import *

from scripts.util import *
refresh = False
upload_s3 = False
log_existing = True
if refresh:
    refresh_schema()

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

pb_df = pd.read_csv("TrainingAnimals_WithSightings_updating.csv")
job_path= "jobs/TrainingAnimals_WithSightings_updating.csv"
# new labels RGB 955 IR 957
# new hotspots 489
# new images RGB 285 IR 683

rgb_dirs = ["/data/raw_data/TrainingAnimals_ColorImages"]
ir_dirs = ["/data/raw_data/TrainingAnimals_ThermalImages", "/data/raw_data/ALL_THERMAL"]
session = Session()

total = len(pb_df)
new_labels_ir = 0
new_labels_rgb = 0
new_hotspots = 0
new_images_rgb = 0
new_images_ir = 0
for i, row in pb_df.iterrows():
    vals = list(row)
    id = vals[0]
    rgb_image_name = vals[1]
    ir_image_name = None if pd.isna(vals[2]) else vals[2]
    pb_id = None if pd.isna(vals[4]) else vals[4]
    hotspot_type = vals[5]
    species_id = vals[5]

    if hotspot_type == "Polar Bear":
        continue

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
    end_date = datetime.now() if removed else None

    if rgb_path is None:
        print("Not found RGB!", rgb_image_name)
    if ir_image_name is not None and ir_path is None:
        print("Not found IR!", ir_image_name)


    rgb_worker_name = YUVAL_WORKER if updated else NOAA_WORKER
    rgb_job_name = YUVAL_JOB if updated else NOAA_JOB
    ir_worker_name = NOAA_WORKER
    ir_job_name = NOAA_JOB
    if is_new:
        rgb_job_name = YUVAL_NEW_JOB

    # open rgb image, compress and save locally
    rgb_im = Image.open(rgb_path)
    comressed_rgb_local_path = os.path.join("/fast/s3/images/rgb/", "c_" + rgb_image_name)
    if not file_exists(comressed_rgb_local_path):
        rgb_im.save(comressed_rgb_local_path, "JPEG", optimize=True, quality=50)

    s3_rgb_path = os.path.join("images/rgb/", rgb_image_name)
    s3_rgb__compressed_path = os.path.join("images/rgb/compressed/", rgb_image_name)
    if upload_s3:
        upoload_s3(s3, s3_client, 'noaa-data', rgb_path, s3_rgb_path)
        upoload_s3(s3, s3_client, 'noaa-data',comressed_rgb_local_path, s3_rgb__compressed_path)

    s3_ir_path = None
    ir_im = None
    ir_exists = ir_image_name is not None and ir_path is not None
    if ir_exists:
        s3_ir_path = os.path.join("images/ir/", ir_image_name)
        if upload_s3:
            upoload_s3(s3, s3_client, 'noaa-data', ir_path, s3_ir_path)
        ir_im = Image.open(ir_path)


    # parse timestamp if possible
    rgb_file_info = parse_chess_filename(rgb_image_name)
    rgb_timestamp = parse_timestamp(rgb_file_info["timestamp"])
    if rgb_timestamp is None:
        print("unable to parse timestamp", rgb_file_info)
    # Insert image if they don't already exist in table
    rgb_db_row_new=get_image(session, rgb_image_name)
    if not rgb_db_row_new:
        rgb_db_row_new= NOAAImage(
            file_name=rgb_image_name,
            file_path=rgb_path,
            type=ImageType.EO,
            width=rgb_im.width,
            height=rgb_im.height,
            depth=rgb_im.layers,
            survey=rgb_file_info['survey'],
            flight=rgb_file_info['flight'],
            cam_position=rgb_file_info['camPos'],
            quality=image_quality,
            foggy=fog,
            timestamp=rgb_timestamp
        )
        session.add(rgb_db_row_new)
        try:
            session.commit()
            new_images_rgb+=1
            print("Inserted Image: %s" % rgb_db_row_new.file_name)
        except:
            session.rollback()
            rgb_db_row_new = get_image(session, rgb_image_name)
            if log_existing: print("Image already exists: %s" % rgb_db_row_new.file_name)
            updates = {}
            old = {}
            if rgb_db_row_new.quality != image_quality and image_quality is not None:
                updates['quality'] = image_quality
                old['quality'] = rgb_db_row_new.quality

                session.query(NOAAImage).filter(NOAAImage.file_name == rgb_image_name).update(updates)
                print("Updated NOAAImage %s" % rgb_image_name, old, updates)
                session.flush()

    ir_db_row_new= get_image(session, ir_image_name)
    if not ir_db_row_new:
        # if ir_image_name is not None and not queries.image_exists(session, ir_image_name):
        if ir_image_name is not None:
            ir_file_info = parse_chess_filename(ir_image_name)
            ir_timestamp = parse_timestamp(ir_file_info["timestamp"])
            ir_db_row_new = NOAAImage(
                file_name=ir_image_name,
                file_path=ir_path,
                type=ImageType.IR,
                width=ir_im.width,
                height=ir_im.height,
                depth=1,
                survey=ir_file_info['survey'],
                flight=ir_file_info['flight'],
                cam_position=rgb_file_info['camPos'],
                timestamp=ir_timestamp
            )
            session.add(ir_db_row_new)

            try:
                session.commit()
                session.flush()
                new_images_ir+=1
                print("Inserted Image: %s" % ir_db_row_new.file_name)
            except:
                session.rollback()
                ir_db_row_new = get_image(session, ir_image_name)
                if log_existing: print("Image already exists: %s" % ir_db_row_new.file_name)

    rgb_job = add_job_if_not_exists(session, rgb_job_name, job_path)
    rgb_worker = add_worker_if_not_exists(session, rgb_worker_name, True)

    ir_job = add_job_if_not_exists(session, ir_job_name, job_path)
    ir_worker = add_worker_if_not_exists(session, ir_worker_name, True)

    sp = get_species(session, species_id)
    if not sp:
        sp = Species(name=species_id)
        session.add(sp)
        try:
            session.commit()
            session.flush()
        except:
            session.rollback()
            sp = get_species(session, species_id)

    # if not species_exists(session, species_id):
    #     species_row = Species(name=species_id)
    #     session.add(species_row)
    # sp = get_species(session, species_id)

    age_class = None
    hotspot_id = None if is_new else hotspot_id
    label_entry_ir = None
    if ir_exists and not is_new and thermal_x is not None and thermal_y is not None:
        label_entry_ir_l = IRLabelEntry(
            image = ir_db_row_new,
            species = sp,
            x1 = thermal_x, # TODO set earlier
            x2 = thermal_x,
            y1 = thermal_y,
            y2 = thermal_y,
            confidence = species_confidence,
            is_shadow = pb_id is not None and pb_id[-1] == "s",
            start_date = datetime.now(),
            end_date= end_date,
            worker = ir_worker,
            job = ir_job
        )
        label_entry_ir = get_existing_ir_label(session, label_entry_ir_l)
        if not label_entry_ir:
            try:
                session.add(label_entry_ir_l)
                session.commit()
                session.flush()
                new_labels_ir+=1
                label_entry_ir = label_entry_ir_l
                print("Insert IR Label:", label_entry_ir_l)
            except:
                session.rollback()
                label_entry_ir = get_existing_ir_label(session, label_entry_ir_l)
                if log_existing: print("IR Label exists id=%d" % label_entry_ir_l.id)

    label_entry_rgb_l = EOLabelEntry(
        image = rgb_db_row_new,
        species = sp,
        x1 = x1, # TODO set earlier
        x2 = x2,
        y1 = y1,
        y2 = y2,
        confidence = species_confidence,
        is_shadow = pb_id is not None and pb_id[-1] == "s",
        start_date = datetime.now(),
        end_date= end_date,
        worker = rgb_worker,
        job = rgb_job
    )
    label_entry_rgb = get_existing_eo_label(session, label_entry_rgb_l)
    if not label_entry_rgb:
        try:
            session.add(label_entry_rgb_l)
            session.flush()
            session.commit()
            new_labels_rgb+=1
            label_entry_rgb = label_entry_rgb_l
            print("Insert RGB Label:", label_entry_rgb_l)
        except:
            session.rollback()
            label_entry_rgb = get_existing_eo_label(session, label_entry_rgb_l)
            if log_existing: print("RGB Label exists id=%d" % label_entry_rgb_l.id)

    if not removed and not (label_entry_rgb is None and label_entry_ir is None):
        l = Sighting(
            # eo_label = None if not label_entry_rgb else label_entry_rgb,
            # ir_label = None if not label_entry_ir or is_new else label_entry_ir,
            ir_label_id = None if not label_entry_ir or is_new else label_entry_ir.id,
            eo_label_id = None if not label_entry_rgb or is_new else label_entry_rgb.id,
            hotspot_id =  None if is_new else hotspot_id,
            species=sp,
            age_class=age_class,
            discriminator=LabelType.TP
        )
        hs = get_existing_sighting(session, l)
        if not hs:
            try:
                session.add(l)
                session.commit()
                session.flush()
                new_hotspots+=1
                print("Insert Hotspot:", l)
            except:
                session.rollback()
                if log_existing: print("Hotspot exists with eo_label=%d ir_label=%d" %(label_entry_rgb.id, label_entry_ir.id))
    print("%d/%d records" % (i, total))
    session.commit()
session.commit()
session.close()
print("new labels RGB %d IR %d\n"
      "new hotspots %d\n"
      "new images RGB %d IR %d\n"%(new_labels_rgb, new_labels_ir,
                                   new_hotspots, new_images_rgb,
                                   new_images_ir))