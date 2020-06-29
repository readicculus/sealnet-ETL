import json
import os

from noaadb.schema.models import Species, FlightMetaHeader, FlightMetaInstruments, FlightMetaEvent, \
    ImageType, NOAAImage
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

def append_header(session, header_dict, fc_id):
    fmh = FlightMetaHeader(
        stamp=safe_int_cast(header_dict['stamp']),
        frame_id=header_dict['frame_id'],
        seq=safe_int_cast(header_dict['seq']),
        flight=fc_id.flight,
        cam=fc_id.cam,
        survey=fc_id.survey
    )
    fmh = session.merge(fmh)
    session.flush()
    return fmh


def add_image(s, path, im_type, fc_id, w=None, h=None,
              is_bigendian=None,step=None,encoding=None,meta_header=None,meta_instrument=None,meta_evt=None):
    if w is None or h is None:
        w, h = get_image_size(path)
    flight, cam, timestamp = image_fn_parser(path)
    name = os.path.basename(path)
    # im = s.query(NOAAImage).filter(NOAAImage.file_name==name).first()
    # if im: return im
    im = NOAAImage(
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
        meta_header=meta_header,
        meta_instrument=meta_instrument,
        meta_evt=meta_evt,
        flight=fc_id.flight,
        cam=fc_id.cam,
        survey=fc_id.survey
    )
    s.add(im)
    return im

def append_meta(session, meta_file,fc_id, eo_path, ir_path):
    if meta_file is None:
        return None, None
    if not os.path.exists(meta_file):
        return None, None
    meta = None
    try:
        with open(os.path.join(meta_file), 'r') as f:
            meta = f.read()
            meta = json.loads(meta)
    except:
        print("Could not read %s" % meta_file)
        return None, None
    ins_obj = None
    if 'ins' in meta:
        ins_meta = meta['ins']
        header_obj = None
        if 'header' in ins_meta:
            header_obj = append_header(session, ins_meta['header'], fc_id)

        ins_obj = FlightMetaInstruments(
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
            meta_header=header_obj,
            flight=fc_id.flight,
            cam=fc_id.cam,
            survey=fc_id.survey
        )
        session.add(ins_obj)
        # session.flush()

    evt_obj = None
    if 'evt' in meta:
        evt_meta = meta['evt']
        header_obj = None
        if 'header' in evt_meta:
            header_obj = append_header(session, evt_meta['header'], fc_id)

        evt_obj = FlightMetaEvent(
            event_port=safe_int_cast(evt_meta.get("event_port")),
            event_num=safe_int_cast(evt_meta.get("event_num")),
            time=safe_float_cast(evt_meta.get("time")),
            meta_header=header_obj,
            flight=fc_id.flight,
            cam=fc_id.cam,
            survey=fc_id.survey
        )
        session.add(evt_obj)
        # session.flush()

    header_obj = None
    if 'rgb' in meta:
        rgb_meta = meta['rgb']
        if 'header' in rgb_meta:
            header_obj = append_header(session, rgb_meta['header'], fc_id)
        add_image(session, eo_path, ImageType.RGB,  fc_id,
                  w=safe_int_cast(rgb_meta.get("width")),
                  h=safe_int_cast(rgb_meta.get("height")),
                  step=safe_int_cast(rgb_meta.get("step")),
                  encoding=rgb_meta["encoding"],
                  is_bigendian=safe_int_cast(rgb_meta.get("is_bigendian")),
                  meta_header=header_obj,
                  meta_instrument=ins_obj,
                  meta_evt=evt_obj)
        # session.add(rgb_obj)
        # session.flush()
    if ins_obj is not None:
        if ins_obj.header_id is None:
            ins_obj.meta_header = header_obj
    if evt_obj is not None:
        if evt_obj.header_id is None:
            evt_obj.meta_header = header_obj
    if 'ir' in meta:
        ir_meta = meta['ir']
        add_image(session, ir_path, ImageType.IR,  fc_id,
                  w=safe_int_cast(ir_meta.get("width")),
                  h=safe_int_cast(ir_meta.get("height")),
                  step=safe_int_cast(ir_meta.get("step")),
                  encoding=ir_meta["encoding"],
                  is_bigendian=safe_int_cast(ir_meta.get("is_bigendian")),
                  meta_header=header_obj,
                  meta_instrument=ins_obj,
                  meta_evt=evt_obj)
        # session.add(ir_obj)
        # session.flush()
