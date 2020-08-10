import json
import logging
import os

from noaadb.schema.models import Species, EventMeta, HeaderMeta, InstrumentMeta, EOImage, IRImage, HeaderGroup
from noaadb.schema.models.survey_data import ImageType
from noaadb.schema.utils.queries import get_species
from import_project.utils.get_image_size import get_image_size
from import_project.utils.util import parse_timestamp



def image_fn_parser(im):
    file_name = os.path.basename(im)
    name_parts = file_name.split('_')
    start_idx = 3
    # fl01 images have slightly  different names
    if name_parts[2] != '2019':
        start_idx = 2
    flight = name_parts[start_idx]
    cam = name_parts[start_idx + 1]
    day = name_parts[start_idx + 2]
    time = name_parts[start_idx + 3]
    timestamp = parse_timestamp(day + time + "GMT")
    return flight, cam, timestamp

def safe_float_cast(s):
    if s is None: return None
    try:
        return float(s)
    except:
        return None

def safe_int_cast(s):
    if s is None: return None
    try:
        return int(s)
    except:
        return None

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

def append_header(session, header_dict, camera):
    fmh = session.query(HeaderMeta).filter_by(stamp=header_dict['stamp']).filter_by(camera=camera).first()
    if fmh:
        return fmh
    stamp = safe_int_cast(header_dict['stamp'])
    fmh = HeaderMeta(
        stamp=stamp,
        frame_id=header_dict['frame_id'],
        seq=safe_int_cast(header_dict['seq']),
        camera=camera
    )
    session.add(fmh)
    # session.commit()
    session.flush()
    return fmh


def add_image(s, path, im_type, w=None, h=None,
              is_bigendian=None,step=None,encoding=None,meta_header=None):
    if path is None:
        return None, False
    if w is None or h is None:
        w, h = get_image_size(path)
    flight, cam, timestamp = image_fn_parser(path)
    name = os.path.basename(path)
    ImageC = EOImage if im_type == ImageType.EO else IRImage
    im = s.query(ImageC).filter(ImageC.file_name==name).first()
    if im: return im, False
    im = ImageC(
        file_name=name,
        file_path=path,
        type=im_type,
        width=w,
        height=h,
        depth=3 if im_type == ImageType.EO else 1,
        timestamp=timestamp,
        is_bigendian=is_bigendian,
        step=step,
        encoding=encoding,
        header_meta=meta_header
    )
    s.add(im)
    return im, True

def append_meta(session, meta_file,camera, eo_path, ir_path, missing_header):
    try:
        with open(os.path.join(meta_file), 'r') as f:
            meta = f.read()
            meta = json.loads(meta)
    except:
        logging.info("Could not find metafile %s" %meta_file)
        meta = missing_header

    eo_header_obj = None
    ir_header_obj = None
    evt_header_obj = None
    ins_header_obj = None
    if 'rgb' in meta and 'header' in meta['rgb']:
        eo_header_obj = append_header(session, meta['rgb']['header'], camera)
    if 'ir' in meta and 'header' in meta['ir']:
        ir_header_obj = append_header(session, meta['ir']['header'], camera)
    if 'evt' in meta and 'header' in meta['evt']:
        evt_header_obj = append_header(session, meta['evt']['header'], camera)
    if 'ins' in meta and 'header' in meta['ins']:
        ins_header_obj = append_header(session, meta['ins']['header'], camera)
    if eo_header_obj is None:
        logging.error("eo_header_obj is None : %s\n" % meta_file)

    # eo header and ir header should be same I think
    # evt header should also be the same because it uses time of event sent not time of image received
    # instrument header is a bit different because i guess it measures and uses the time of measurment as the header
    if ir_header_obj is not None and ir_header_obj.stamp != 0:
        logging.error("ir_header_obj.stamp != 0 : %s\n" % meta_file)

    has_ir_header = ir_header_obj is not None and ir_header_obj.stamp != 0
    has_eo_header = eo_header_obj is not None
    has_evt_header = evt_header_obj is not None
    has_ins_header = ins_header_obj is not None
    logging.info("has_ir_header: %s - "
                 "has_eo_header: %s - "
                 "has_evt_header: %s - "
                 "has_ins_header: %s" %  (has_ir_header, has_eo_header, has_evt_header, has_ins_header))


    if not has_eo_header and not has_ir_header:
        ir_header_obj = evt_header_obj if evt_header_obj else ins_header_obj
        eo_header_obj = evt_header_obj if evt_header_obj else ins_header_obj
        used_header = 'evt_header' if evt_header_obj else 'ins_header'
        logging.error("ERROR: no ir_header or eo_header %s" % meta_file)
        logging.info("USING: for ir_header and eo_header using %s" % used_header)
    elif not has_eo_header:
        eo_header_obj = ir_header_obj
        logging.info("USING: for eo_header using ir_header")
    elif not has_ir_header:
        ir_header_obj = eo_header_obj
        logging.info("USING: for ir_header using eo_header")

    if not evt_header_obj:
        evt_header_obj = eo_header_obj
        logging.info("USING: for evt_header using eo_header")
    if not ins_header_obj:
        ins_header_obj = eo_header_obj
        logging.info("USING: for ins_header using eo_header")

    eo_im, ir_im,rgb_meta,ir_meta = None, None, None, None
    rgb_meta = None
    if 'rgb' in meta:
        rgb_meta = meta['rgb']
    else:
        logging.info("NO EO PATH")

    eo_im, eo_added = add_image(session, eo_path, ImageType.EO,
              w=safe_int_cast(rgb_meta.get("width")) if rgb_meta else None,
              h=safe_int_cast(rgb_meta.get("height")) if rgb_meta else None,
              step=safe_int_cast(rgb_meta.get("step")) if rgb_meta else None,
              encoding=rgb_meta["encoding"] if rgb_meta and "encoding" in rgb_meta else 'bayer_grbg8',
              is_bigendian=safe_int_cast(rgb_meta.get("is_bigendian")) if rgb_meta else None,
              meta_header=eo_header_obj)
    if eo_im:
        if eo_added:
            logging.info("ADDED: %s" % eo_path)
        else:
            logging.info("EXISTS: %s" % eo_path)
    ir_meta = None
    if 'ir' in meta:
        ir_meta = meta['ir']
    else:
        logging.info("NO IR PATH")

    ir_im, ir_added = add_image(session, ir_path, ImageType.IR,
                             w=safe_int_cast(ir_meta.get("width")) if ir_meta else None,
                             h=safe_int_cast(ir_meta.get("height")) if ir_meta else None,
                             step=safe_int_cast(ir_meta.get("step")) if ir_meta else None,
                             encoding=ir_meta["encoding"] if ir_meta and "encoding" in ir_meta else 'mono16',
                             is_bigendian=safe_int_cast(ir_meta.get("is_bigendian")) if ir_meta else None,
                             meta_header=ir_header_obj)
    if ir_im:
        if ir_added:
            logging.info("ADDED: %s" % ir_path)
        else:
            logging.info("EXISTS: %s" % ir_path)


    if 'ins' in meta:
        ins_meta = meta['ins']
        ins_obj = session.query(InstrumentMeta).filter_by(header_meta=ins_header_obj).first()
        if not ins_obj:
            ins_obj = InstrumentMeta(
                track_angle=safe_float_cast(ins_meta.get("track_angle")),
                angular_rate_x=safe_float_cast(ins_meta.get("angular_rate_x")),
                angular_rate_y=safe_float_cast(ins_meta.get("angular_rate_y")),
                angular_rate_z=safe_float_cast(ins_meta.get("angular_rate_z")),
                down_velocity=safe_float_cast(ins_meta.get("down_velocity")),
                pitch=safe_float_cast(ins_meta.get("pitch")),
                altitude=safe_float_cast(ins_meta.get("altitude")),
                north_velocity=safe_float_cast(ins_meta.get("north_velocity")),
                acceleration_y=safe_float_cast(ins_meta.get("acceleration_y")),
                gnss_status=safe_int_cast(ins_meta.get("gnss_status")),
                longitude=safe_float_cast(ins_meta.get("longitude")),
                roll=safe_float_cast(ins_meta.get("roll")),
                acceleration_x=safe_float_cast(ins_meta.get("acceleration_x")),
                align_status=safe_int_cast(ins_meta.get("align_status")),
                total_speed=safe_float_cast(ins_meta.get("total_speed")),
                time=safe_float_cast(ins_meta.get("time")),
                latitude=safe_float_cast(ins_meta.get("latitude")),
                heading=safe_float_cast(ins_meta.get("heading")),
                east_velocity=safe_float_cast(ins_meta.get("east_velocity")),
                acceleration_z=safe_float_cast(ins_meta.get("acceleration_z")),
                header_meta=ins_header_obj
            )
            session.add(ins_obj)

    evt_obj = None
    if 'evt' in meta:
        evt_meta = meta['evt']
        evt_obj = session.query(EventMeta).filter_by(header_meta=evt_header_obj).first()
        if not evt_obj:
            evt_obj = EventMeta(
                event_port=safe_int_cast(evt_meta.get("event_port")),
                event_num=safe_int_cast(evt_meta.get("event_num")),
                time=safe_float_cast(evt_meta.get("time")),
                header_meta=evt_header_obj
            )
            session.add(evt_obj)

    hg = HeaderGroup(eo_image = eo_im, ir_image = ir_im, evt_header_meta = evt_obj)
    session.add(hg)
    return eo_added, ir_added

def setup_logger(log_file):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(filename=log_file, filemode='w',
                        format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO)
