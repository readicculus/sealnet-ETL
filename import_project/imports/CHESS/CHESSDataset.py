import glob
import os
import pandas as pd

from import_project.imports.import_models import StandardFlightDataset
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

# class CHESSDetectionsDataset():
#     def __init__(self, flight):
#         df = pd.read_csv("/home/yuval/Documents/XNOR/sealnet-ETL/import_project/data/TrainingAnimals_WithSightings_updating.csv")
#         self.df=df[df.color_image.str.contains('_%s_' % flight)]
# 
#     def iterrows(self):
#         return self.df.iterrows()

class CHESSDataset(StandardFlightDataset):
    def __init__(self, color_dir, ir_dir, flight, eo_df=None, ir_df= None):
        self.flight = flight
        self.color_dir = color_dir
        self.ir_dir = ir_dir
        self.flight_cams = {}
        self.survey = 'CHESS_2016'
        self.cams = get_cams(color_dir, self.flight)
        # df = pd.read_csv("/home/yuval/Documents/XNOR/sealnet-ETL/import_project/data/TrainingAnimals_WithSightings_updating.csv")
        self.eo_df = None
        self.ir_df = None
        if eo_df is not None:
            self.eo_df=eo_df[eo_df.image.str.contains('_%s_' % flight)]
            self.eo_df.columns = [str(col) + '_eo' for col in self.eo_df.columns]

        if ir_df is not None:
            self.ir_df=ir_df[ir_df.image.str.contains('_%s_' % flight)]
            self.ir_df.columns = [str(col) + '_ir' for col in self.ir_df.columns]

    def get_cam_transform(self, cam_name):
        return None

    def get_cam_eo_detections_file(self, cam_name):
        return None

    def get_cam_ir_detections_file(self, cam_name):
        return None

    def get_cam_eo_detections(self, cam_name):
        cam_only = self.eo_df[self.eo_df.image_eo.str.contains('_%s_' % cam_name)]
        return cam_only

    def get_cam_ir_detections(self, cam_name):
        cam_only = self.ir_df[self.ir_df.image_ir.str.contains('_%s_' % cam_name)]
        return cam_only

    def id(self):
        return self.flight

    def get_cam_names(self):
        return self.cams

    def get_cam_id(self, cam_name):
        return cam_name

    def get_cam_meta_files(self,cam_name):
        return []

    def get_cam_ir_images(self, cam_name):
        ir_files = glob.glob(os.path.join(self.ir_dir, '*_%s_%s_*_THERM-16-BIT.PNG' % (self.flight, cam_name)))
        return ir_files

    def get_cam_eo_images(self, cam_name):
        eo_files = glob.glob(os.path.join(self.color_dir, '*_%s_%s_*_COLOR-8-BIT.JPG' % (self.flight, cam_name)))
        return eo_files

    def ext_id(self, ext):
        if ext == '.JPG':
            return 'eo'
        if ext == '.PNG':
            return 'ir'
        if ext == '.json':
            return 'meta'

    def get_cam_eo_ir_meta_matches(self, cam_name):
        eo_list = self.get_cam_eo_images(cam_name)
        ir_list = self.get_cam_ir_images(cam_name)
        meta_list = self.get_cam_meta_files(cam_name)
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

chess_datasets = []
flights = get_flights(color_dir)
flights=sorted(flights)
for flight in flights:
    dataset = CHESSDataset(color_dir, ir_dir, flight, None, None)
    chess_datasets.append(dataset)

for d in chess_datasets:
    matches=d.get_cam_eo_ir_meta_matches('*')
    print('%s - %d ims' %(d.id(), len(matches)))
