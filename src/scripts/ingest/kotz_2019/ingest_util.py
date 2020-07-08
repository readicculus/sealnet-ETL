import json
import os

from noaadb.schema.models import Species, FlightMetaEvent, \
    ImageType, HeaderMeta, InstrumentMeta, EOImage, IRImage
from noaadb.schema.queries import get_species
from scripts.get_image_size import get_image_size
from scripts.util import parse_timestamp


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
        return
    if w is None or h is None:
        w, h = get_image_size(path)
    flight, cam, timestamp = image_fn_parser(path)
    name = os.path.basename(path)
    ImageC = EOImage if im_type == ImageType.EO else IRImage
    im = s.query(ImageC).filter(ImageC.file_name==name).first()
    if im: return im
    im = ImageC(
        file_name=name,
        file_path=path,
        type=im_type,
        width=w,
        height=h,
        depth=3,
        timestamp=timestamp,
        is_bigendian=is_bigendian,
        step=step,
        encoding=encoding,
        header_meta=meta_header
    )
    s.add(im)
    return im

def append_meta(session, meta_file,camera, eo_path, ir_path):
    if meta_file is None:
        return None, None
    if not os.path.exists(meta_file) or '_ST_' in meta_file:
        return None, None
    meta = None
    try:
        with open(os.path.join(meta_file), 'r') as f:
            meta = f.read()
            meta = json.loads(meta)
    except:
        print("Could not read %s" % meta_file)
        return None, None
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



    grouped_header = eo_header_obj or ir_header_obj or evt_header_obj or ins_header_obj
    if 'rgb' in meta:
        rgb_meta = meta['rgb']
        add_image(session, eo_path, ImageType.EO,
                  w=safe_int_cast(rgb_meta.get("width")),
                  h=safe_int_cast(rgb_meta.get("height")),
                  step=safe_int_cast(rgb_meta.get("step")),
                  encoding=rgb_meta["encoding"],
                  is_bigendian=safe_int_cast(rgb_meta.get("is_bigendian")),
                  meta_header=grouped_header)
    if 'ir' in meta:
        ir_meta = meta['ir']
        add_image(session, ir_path, ImageType.IR,
                  w=safe_int_cast(ir_meta.get("width")),
                  h=safe_int_cast(ir_meta.get("height")),
                  step=safe_int_cast(ir_meta.get("step")),
                  encoding=ir_meta["encoding"],
                  is_bigendian=safe_int_cast(ir_meta.get("is_bigendian")),
                  meta_header=grouped_header)

    if 'ins' in meta:
        ins_meta = meta['ins']
        ins_obj = session.query(InstrumentMeta).filter_by(header_meta=grouped_header).first()
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
                header_meta=grouped_header
            )
            session.add(ins_obj)

    evt_obj = None
    if 'evt' in meta:
        evt_meta = meta['evt']
        evt_obj = session.query(FlightMetaEvent).filter_by(header_meta=grouped_header).first()
        if not evt_obj:
            evt_obj = FlightMetaEvent(
                event_port=safe_int_cast(evt_meta.get("event_port")),
                event_num=safe_int_cast(evt_meta.get("event_num")),
                time=safe_float_cast(evt_meta.get("time")),
                header_meta=grouped_header
            )
            session.add(evt_obj)

