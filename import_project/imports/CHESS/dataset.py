import glob
import os
import pandas as pd

from import_project.utils.util import parse_chess_filename


def get_cams(dir, flight):
    im_list = glob.glob(os.path.join(dir, '*_%s_*_COLOR-8-BIT.JPG'%flight))
    cams = set()
    for im in im_list:
        res = parse_chess_filename(os.path.basename(im))
        cams.add(res['camPos'])
    return cams

def get_flights(dir):
    im_list = glob.glob(os.path.join(dir, '*_COLOR-8-BIT.JPG'))
    flights = set()
    for im in im_list:
        res = parse_chess_filename(os.path.basename(im))
        flights.add(res['flight'])
    return flights

class CHESSDataset():
    def __init__(self, color_dir, ir_dir, flight, cam_id):
        df = pd.read_csv("/home/yuval/Documents/XNOR/sealnet-ETL/import_project/data/TrainingAnimals_WithSightings_updating.csv")
        self.df=df[df.color_image.str.contains('_%s_' % flight)]
        self.cam_id = cam_id
        self.flight = flight
        self.color_dir = color_dir
        self.ir_dir = ir_dir
        self.flight_cams = {}
        self.survey = 'CHESS_2016'
        self.cams = get_cams(color_dir, self.flight)


    def get_cam_transform(self, cam_name):
        return None

    def get_cam_eo_detections_file(self, cam_name):
        return None

    def get_cam_ir_detections_file(self, cam_name):
        return None

    def id(self):
        return self.flight

    def get_cam_names(self):
        return self.cams

    def get_cam_id(self, cam_name):
        return cam_name

    def get_cam_meta_files(self):
        return []

    def get_cam_ir_images(self):
        return []

    def get_cam_eo_images(self):
        eo_files = glob.glob(os.path.join(self.color_dir, '*_%s_*_COLOR-8-BIT.JPG' % self.flight))
        return eo_files

    def ext_id(self, ext):
        if ext == '.JPG':
            return 'eo'
        if ext == '.tif':
            return 'ir'
        if ext == '.json':
            return 'meta'

    def get_cam_eo_ir_meta_matches(self):
        eo_list = self.get_cam_eo_images()
        ir_list = self.get_cam_ir_images()
        meta_list = self.get_cam_meta_files()
        match_dict = {}
        for im in eo_list+ir_list+meta_list:
            ext = os.path.splitext(im)[1]
            im_no_ext = '_'.join(im.split('_')[:-1])
            base_name = os.path.basename(im_no_ext)
            if not base_name in match_dict:
                match_dict[base_name] = {}
            match_dict[base_name][self.ext_id(ext)] = im

        return list(match_dict.values())
color_dir = '/data/raw_data/TrainingAnimals_ColorImages'
ir_dir = '/data/raw_data/ALL_THERMAL'

CHESS_datasets = []
flights = get_flights(color_dir)
flights=sorted(flights)
for flight in flights:
    d = CHESSDataset(color_dir, ir_dir, flight, 'cam')
    CHESS_datasets.append(d)


for d in CHESS_datasets:
    matches=d.get_cam_eo_ir_meta_matches()
    print('%s - %d ims' %(d.id(), len(matches)))
    a=1