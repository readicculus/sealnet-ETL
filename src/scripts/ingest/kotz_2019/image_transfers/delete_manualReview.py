import logging
import os
import pandas as pd

logging.basicConfig(filename='/data2/2019/fl07/delete_manualReview.log', filemode='a',
                    format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.DEBUG)
fl07_CENT = ('/data2/2019/fl07/CENT',
             '/data2/2019/fl07/CENT/2019TestF7C_tinyYolo_eo_20200219_processed.csv',
             '/data2/2019/fl07/CENT/2019TestF7C_tinyYolo_ir_20200219_projected.csv',
             '/data2/2019/fl07/2019TestF7C_tinyYolo_eo_20200219_original_imagesRGB_manualReview.txt',[])
fl07_LEFT = ('/data2/2019/fl07/LEFT',
             '/data2/2019/fl07/LEFT/2019TestF7L_tinyYolo_eo_20200221_processed.csv',
             '/data2/2019/fl07/LEFT/2019TestF7L_tinyYolo_ir_20200221_projected.csv',
             '/data2/2019/fl07/2019TestF7L_tinyYolo_eo_20200221_original_imagesRGB_manualReview.txt',[])
to_remove = [fl07_CENT, fl07_LEFT]
files_to_rm = []
logging.info(fl07_CENT)
logging.info(fl07_LEFT)


for fl in to_remove:
    with open(fl[3]) as file_in:
        for line in file_in:
            name = line.strip()
            im_no_ext = '_'.join(name.split('_')[:-1])
            ir = im_no_ext + '_ir.tif'
            rgb = im_no_ext + '_rgb.jpg'
            json = im_no_ext + '_meta.json'
            fl[4].append(os.path.join(fl[0], ir))
            fl[4].append(os.path.join(fl[0], rgb))
            fl[4].append(os.path.join(fl[0], json))

deleted = 0
fl07_CENT_eo_df = pd.read_csv(fl07_CENT[1], header=None, comment='#')
fl07_CENT_ir_df = pd.read_csv(fl07_CENT[2], header=None, comment='#')
fl07_LEFT_eo_df = pd.read_csv(fl07_LEFT[1], header=None, comment='#')
fl07_LEFT_ir_df = pd.read_csv(fl07_LEFT[2], header=None, comment='#')
cols = ['id', 'image', 'num_dets', 'x1', 'y1', 'x2', 'y2', 'confidence', 'idk', 'species', 'conf2']
fl07_CENT_eo_df.columns = cols
fl07_CENT_ir_df.columns = cols
fl07_LEFT_eo_df.columns = cols
fl07_LEFT_ir_df.columns = cols

def remove_file(f):
    if os.path.exists(f):
        os.remove(f)
        logging.info("Deleted %s" % f)
    else:
        logging.info("Could not find %s" % f)

def remove_from_csv(files, df):
    files = [os.path.basename(file) for file in files]
    idxs_to_rm = []
    df['image'] = df['image'].str.replace('rgb.tif', 'rgb.jpg')

    for i, row in df.iterrows():
        if os.path.basename(row['image']) in files:
            idxs_to_rm.append(i)
    print("Removed %d rows" % len(idxs_to_rm))
    new = df.drop(idxs_to_rm)
    print("Verify %d rows removed" % (df.shape[0] - new.shape[0]))
    return new


if False:
    files_to_rm = fl07_CENT[4] + fl07_LEFT[4]
    for f in files_to_rm:
        remove_file(f)
        deleted+=1

# remove these images from the csv
fl07_LEFT_eo_df=remove_from_csv(fl07_LEFT[4], fl07_LEFT_eo_df)
# fl07_LEFT_ir_df=remove_from_csv(fl07_LEFT[4], fl07_LEFT_ir_df)

fl07_CENT_eo_df=remove_from_csv(fl07_CENT[4], fl07_CENT_eo_df)
# fl07_CENT_ir_df=remove_from_csv(fl07_CENT[4], fl07_CENT_ir_df)

# Save the new dataframes
fl07_LEFT_eo_df.to_csv(fl07_LEFT[1], sep=',', encoding='utf-8', header=False, index=False)
# fl07_LEFT_ir_df.to_csv(fl07_LEFT[2], sep=',', encoding='utf-8', header=False, index=False)

fl07_CENT_eo_df.to_csv(fl07_CENT[1], sep=',', encoding='utf-8', header=False, index=False)
# fl07_CENT_ir_df.to_csv(fl07_CENT[2], sep=',', encoding='utf-8', header=False, index=False)
logging.info("Deleted %d files", deleted)

