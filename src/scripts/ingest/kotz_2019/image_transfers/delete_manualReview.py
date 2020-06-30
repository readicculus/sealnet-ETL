import logging
import os

logging.basicConfig(filename='/data2/2019/fl07/delete_manualReview.log', filemode='a',
                    format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.DEBUG)
fl07_CENT = ('/data2/2019/fl07/CENT', '/data2/2019/fl07/2019TestF7C_tinyYolo_eo_20200219_original_imagesRGB_manualReview.txt')
fl07_LEFT = ('/data2/2019/fl07/LEFT', '/data2/2019/fl07/2019TestF7L_tinyYolo_eo_20200221_original_imagesRGB_manualReview.txt')
to_remove = [fl07_CENT, fl07_LEFT]
files_to_rm = []
logging.info(fl07_CENT)
logging.info(fl07_LEFT)

for fl in to_remove:
    with open(fl[1]) as file_in:
        for line in file_in:
            name = line.strip()
            im_no_ext = '_'.join(name.split('_')[:-1])
            ir = im_no_ext + '_ir.tif'
            rgb = im_no_ext + '_rgb.jpg'
            json = im_no_ext + '_meta.json'
            files_to_rm.append(os.path.join(fl[0], ir))
            files_to_rm.append(os.path.join(fl[0], rgb))
            files_to_rm.append(os.path.join(fl[0], json))

deleted = 0
for f in files_to_rm:
    if os.path.exists(f):
        os.remove(f)
        logging.info("Deleted %s" % f)
        deleted += 1
    else:
        logging.info("Could not find %s" % f)
logging.info("Deleted %d files", deleted)

x=1