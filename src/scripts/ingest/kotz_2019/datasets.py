import glob
import os


class FlightCam():
    def __init__(self, dir, eo_label_file, ir_label_file, ignore=False):
        self.dir = dir
        self.eo_label_file = eo_label_file
        self.ir_label_file = ir_label_file
        self.ignore = ignore
        self.id_dict = {"CENT": "C", "LEFT": "L", "RIGHT": "R"}
    def id(self):
        return self.id_dict[self.dir]

class KotzFlightDataset():
    def __init__(self, root_dir, flight, flight_cam_list):
        self.root_dir = root_dir
        self.flight = flight
        self.dir = os.path.join(self.root_dir, self.flight)
        self.flight_cams = {}
        for cam in flight_cam_list:
            self.flight_cams[cam.dir] = cam

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


fl06_C = FlightCam('CENT','x', 'x')
fl06_L = FlightCam('LEFT','x', 'x')
fl06_R = FlightCam('RIGHT','x', 'x')
fl06_dataset = KotzFlightDataset('/data2/2019/', 'fl06', [fl06_C, fl06_L, fl06_R])

fl05_C = FlightCam('CENT','x', 'x')
fl05_L = FlightCam('LEFT','x', 'x')
fl05_R = FlightCam('RIGHT','x', 'x')
fl05_dataset = KotzFlightDataset('/data2/2019/', 'fl05', [fl05_C, fl05_L, fl05_R])

fl04_C = FlightCam('CENT','x', 'x')
fl04_L = FlightCam('LEFT','x', 'x')
fl04_R = FlightCam('RIGHT','x', 'x')
fl04_dataset = KotzFlightDataset('/data2/2019/', 'fl04', [fl04_C, fl04_L, fl04_R])

fl01_C = FlightCam('CENT','2019TestF1C_tinyYolo_eo_20190813_processed.csv', '2019TestF1C_tinyYolo_ir_20190813_projected.csv')
fl01_dataset = KotzFlightDataset('/data2/2019/', 'fl01', [fl01_C])



kotz_datasets = [fl06_dataset, fl05_dataset, fl04_dataset, fl01_dataset]