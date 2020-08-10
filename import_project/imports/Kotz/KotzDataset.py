import glob
import os
import pandas as pd

from import_project.imports.import_models import StandardFlightDataset


class FlightCam():
    def __init__(self, dir, eo_label_file, ir_label_file, transform=None):
        self.dir = dir
        self.eo_label_file = eo_label_file
        self.ir_label_file = ir_label_file
        self.id_dict = {"CENT": "C", "LEFT": "L", "RIGHT": "R"}
        self.transform = transform

    def id(self):
        return self.id_dict[self.dir]

    def get_eo_label_file(self, path):
        if self.eo_label_file is None:
            return None
        return os.path.join(path, self.dir, self.eo_label_file)

    def get_transform(self):
        return self.transform

    def get_ir_label_file(self, path):
        if self.ir_label_file is None:
            return None
        return os.path.join(path, self.dir, self.ir_label_file)

class KotzFlightDataset(StandardFlightDataset):
    def __init__(self, root_dir, flight, flight_cam_list):
        self.root_dir = root_dir
        self.flight = flight
        self.dir = os.path.join(self.root_dir, self.flight)
        self.flight_cams = {}
        for cam in flight_cam_list:
            self.flight_cams[cam.dir] = cam

    def get_cam_transform(self, cam_name):
        cam = self.flight_cams[cam_name]
        return cam.get_transform()

    def get_cam_eo_detections_file(self, cam_name):
        cam = self.flight_cams[cam_name]
        return cam.get_eo_label_file(self.dir)

    def get_cam_ir_detections_file(self, cam_name):
        cam = self.flight_cams[cam_name]
        return cam.get_ir_label_file(self.dir)

    def get_cam_eo_detections(self, cam_name):
        f = self.get_cam_eo_detections_file(cam_name)
        eo_df = pd.read_csv(f, header=None, comment='#')
        eo_df.columns = ['id', 'image', 'num_dets', 'x1', 'y1', 'x2', 'y2', 'confidence', 'idk', 'species', 'conf2']
        eo_df.columns = [str(col) + '_eo' for col in eo_df.columns]
        eo_df['image_eo'] = eo_df['image_eo'].str.replace('rgb.tif', 'rgb.jpg')
        int_cols_eo = ['x1_eo', 'y1_eo', 'x2_eo', 'y2_eo']
        eo_df[int_cols_eo] = eo_df[int_cols_eo].round()
        eo_df.loc[eo_df['confidence_eo'] > 1, 'confidence_eo'] = 1
        return eo_df

    def get_cam_ir_detections(self, cam_name):
        f = self.get_cam_ir_detections_file(cam_name)
        if f is None:
            return None
        ir_df = pd.read_csv(f, header=None, comment='#')
        ir_df.columns = ['id', 'image', 'num_dets', 'x1', 'y1', 'x2', 'y2', 'conf2', 'idk', 'species', 'confidence']
        ir_df.columns = [str(col) + '_ir' for col in ir_df.columns]
        int_cols_ir = ['x1_ir', 'y1_ir', 'x2_ir', 'y2_ir']
        ir_df[int_cols_ir] = ir_df[int_cols_ir].round()
        ir_df.loc[ir_df['confidence_ir'] > 1, 'confidence_ir'] = 1
        return ir_df

    def id(self):
        return self.flight

    def get_cam_names(self):
        return list(self.flight_cams.keys())

    def get_cam_id(self, cam_name):
        cam = self.flight_cams[cam_name]
        return cam.id()

    def get_cam_meta_files(self, cam_name):
        cam = self.flight_cams[cam_name]
        cam_data_dir = os.path.join(self.dir, cam.dir)
        json_meta_files = glob.glob(os.path.join(cam_data_dir, '*.json'))
        return json_meta_files

    def get_cam_ir_images(self, cam_name):
        cam = self.flight_cams[cam_name]
        cam_data_dir = os.path.join(self.dir, cam.dir)
        ir_files = glob.glob(os.path.join(cam_data_dir, '*ir.tif'))
        return ir_files

    def get_cam_eo_images(self, cam_name):
        cam = self.flight_cams[cam_name]
        cam_data_dir = os.path.join(self.dir, cam.dir)
        eo_files = glob.glob(os.path.join(cam_data_dir, '*rgb.jpg'))
        return eo_files

    def get_eo_ir_merged_detections(self, cam_name):
        eo_df = self.get_cam_eo_detections(cam_name)
        ir_df = self.get_cam_ir_detections(cam_name)
        has_ir = ir_df is not None
        data = None
        if not has_ir:
            data = eo_df
        else:
            merged = pd.merge(eo_df, ir_df, left_on='id_eo', right_on='id_ir', how='left')
            merged.drop(['id_ir', 'conf2_ir', 'conf2_eo', 'species_ir', 'idk_ir', 'idk_eo', 'num_dets_ir'], axis=1,
                        inplace=True)
            if len(merged) - merged.image_ir.isnull().sum() - len(ir_df) != 0:
                print("len(merged) - merged.image_ir.isnull().sum() - len(ir_df) = %d" % (
                        len(merged) - merged.image_ir.isnull().sum() - len(ir_df)))
            data = merged

        data.rename(columns={'species_eo': 'species', 'id_eo': 'id'}, inplace=True)
        return data, has_ir

    def ext_id(self, ext):
        if ext == '.jpg':
            return 'eo'
        if ext == '.tif':
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

output_transform_Left = '/home/yuval/Documents/XNOR/sealnet/models/darknet/pydn/register/output_transform_Left.h5'
Flight5C_gmb = '/home/yuval/Documents/XNOR/sealnet/models/darknet/pydn/register/Kotz-2019-Flight5C_gmb.h5'
output_transform_4Center = '/home/yuval/Documents/XNOR/sealnet/models/darknet/pydn/register/output_transform_4Center.h5'
DATASET_ROOT = '/data2/2019/'


fl01_C = FlightCam('CENT','2019TestF1C_tinyYolo_eo_20190813_processed.csv',
                   '2019TestF1C_tinyYolo_ir_20190813_projected.csv')
fl01_dataset = KotzFlightDataset(DATASET_ROOT, 'fl01', [fl01_C])


## FINALIZED
#   == FL07 ==
fl07_C = FlightCam('CENT',
                   '2019TestF7C_tinyYolo_eo_20200219_processed.csv',
                   '2019TestF7C_tinyYolo_ir_20200219_projected.csv',
                   transform=Flight5C_gmb)
fl07_L = FlightCam('LEFT',
                   '2019TestF7L_tinyYolo_eo_20200221_processed.csv',
                   '2019TestF7L_tinyYolo_ir_20200221_projected.csv',
                   transform=output_transform_Left)
fl07_dataset = KotzFlightDataset(DATASET_ROOT, 'fl07', [fl07_C, fl07_L])

#   == FL06 ==
fl06_C = FlightCam('CENT',
                   '2019TestF6C_tinyYolo_eo_20191205_processed.csv',
                   '2019TestF6C_tinyYolo_ir_20191205_projected.csv',
                   transform=Flight5C_gmb) # finalized
fl06_L = FlightCam('LEFT','2019TestF6L_tinyYolo_eo_20191205_processed.csv',
                   '2019TestF6L_tinyYolo_ir_20191205_projected.csv',
                   transform=output_transform_Left) # finalized
fl06_R = FlightCam('RIGHT',None,None)
fl06_dataset = KotzFlightDataset(DATASET_ROOT, 'fl06', [fl06_C, fl06_L, fl06_R])

#   == FL05 ==
fl05_C = FlightCam('CENT',
                   '2019TestF5C_tinyYolo_eo_20190905_processed.csv',
                   '2019TestF5C_tinyYolo_ir_20190905_projected.csv',
                   transform=Flight5C_gmb) # finalized
fl05_L = FlightCam('LEFT',
                   '2019TestF5L_tinyYolo_eo_20190905_processed.csv',
                   '2019TestF5L_tinyYolo_ir_20190905_projected.csv',
                   transform=output_transform_Left) # finalized
fl05_R = FlightCam('RIGHT', None, None)
fl05_dataset = KotzFlightDataset(DATASET_ROOT, 'fl05', [fl05_C, fl05_L, fl05_R])

#   == FL04 ==
fl04_C = FlightCam('CENT',
                   '2019TestF4C_tinyYolo_eo_20190904_processed.csv',
                   '2019TestF4C_tinyYolo_ir_20190904_projected.csv',
                   transform=output_transform_4Center) # finalized
fl04_L = FlightCam('LEFT',
                   '2019TestF4L_tinyYolo_eo_20190905_processed.csv',
                   '2019TestF4L_tinyYolo_ir_20190905_projected.csv',
                   transform=output_transform_Left) # finalized
fl04_R = FlightCam('RIGHT',None,None)
fl04_dataset = KotzFlightDataset(DATASET_ROOT, 'fl04', [fl04_C, fl04_L, fl04_R])

#   == FL01 ==


kotz_datasets = [fl07_dataset, fl06_dataset, fl05_dataset, fl04_dataset, fl01_dataset]