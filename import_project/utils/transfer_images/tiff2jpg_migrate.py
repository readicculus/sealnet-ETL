import pickle
import subprocess

import os
from shutil import copyfile
import sys
import logging
class MigrateHelper():
    def __init__(self, rootdir, newroot, ignore_paths, log_file, pkl_file, swap_LR = False):
        self.rootdir=rootdir
        self.newroot=newroot
        self.ignore_paths=ignore_paths
        self.log_file=log_file
        self.pkl_file=pkl_file
        self.swap_LR = swap_LR

# TODO
# fl07_C_L = MigrateHelper('/media/yuval/BASHFUL/fl07/',
#               '/data2/2019/fl07/',
#               ['RGHT'],
#               '/data2/2019/fl07_C_L.log',
#               '/data2/2019/fl07_C_L.pkl')


# fl06_R = MigrateHelper('/media/yuval/BASHFUL/fl06/',
#               '/data2/2019/fl06/',
#               [],
#               '/data2/2019/fl06_R.log',
#               '/data2/2019/fl06_R.pkl')

# fl06_C_L = MigrateHelper('/media/yuval/DOC/fl06/',
#               '/data2/2019/fl06/',
#               [],
#               '/data2/2019/fl06_C_L.log',
#               '/data2/2019/fl06_C_L.pkl')

#fl05
# fl05_C_L_R = MigrateHelper('/media/yuval/DOC/fl05/',
#               '/data2/2019/fl05/',
#               [],
#               '/data2/2019/fl05_C_L_R.log',
#               '/data2/2019/fl05_C_L_R.pkl')
# #fl04
fl04_C_L_R = MigrateHelper('/media/yuval/DOC/fl04/',
              '/data2/2019/fl04/',
              [],
              '/data2/2019/fl04_C_L_R.log',
              '/data2/2019/fl04_C_L_R.pkl', swap_LR=True)
drive_DOC = [fl04_C_L_R]


def copy_file(src, dst):
    global total_size
    total_size += os.path.getsize(src)
    global num_files
    num_files += 1
    copyfile(src, dst)

total_size = 0
num_files = 0
compressed = 0

for flight in drive_DOC:
    log_file = flight.log_file
    rootdir = flight.rootdir
    newroot = flight.newroot
    pkl_file = flight.pkl_file
    logging.basicConfig(filename=log_file, filemode='a',
                        format = '%(asctime)s %(message)s',
                        datefmt = '%m/%d/%Y %I:%M:%S %p',
                        level=logging.DEBUG)
    print("Now logging to %s" % log_file)

    ignore_paths = []
    logging.info('From: %s', rootdir)
    logging.info('To: %s', newroot)
    logging.info('Ignoring: %s', str(ignore_paths))

    if not os.path.exists(rootdir):
        raise("%s does not eixst" % rootdir)
    if not os.path.exists(newroot):
        print("Creating %s" % newroot)
        os.makedirs(newroot)

    folder_src_dst_map = {}
    if os.path.exists(pkl_file):
        with open(pkl_file, "rb") as f:
            folder_src_dst_map = pickle.load(f)
            # del folder_src_dst_map['CENT/TooSmall']
    else:
        for subdir, dirs, files in os.walk(rootdir):
            folder = subdir[len(rootdir):]
            newdir = os.path.join(newroot, folder)
            if newdir in ignore_paths:
                continue

            if not folder in folder_src_dst_map:
                folder_src_dst_map[folder] = {}
            for file in files:
                newloc = os.path.join(newdir, file)
                oldloc = os.path.join(subdir, file)
                file_name, file_extension = os.path.splitext(file)
                if file_extension == ".tif":
                    im_type = file_name.split("_")[-1]
                    if im_type == "rgb":
                        base = '.'.join(newloc.split('.')[:-1])
                        newloc = base+".jpg"
                folder_src_dst_map[folder][oldloc] = {'dst':newloc,'complete':False}
                if flight.swap_LR:
                    dst = folder_src_dst_map[folder][oldloc]['dst']
                    if 'RGHT' in dst:
                        dst = dst.replace('RGHT', 'LEFT')
                        dst = dst.replace('_R_', '_L_')
                    elif 'LEFT' in dst:
                        dst = dst.replace('LEFT', 'RIGHT')
                        dst = dst.replace('_L_', '_R_')
                    folder_src_dst_map[folder][oldloc]['dst'] = dst
    i = 0
    print(folder_src_dst_map.keys())
    for folder in folder_src_dst_map:
        if folder == 'LEFT' or folder == "CENT":
            continue
        total_size = num_files = compressed = 0
        newdir = os.path.join(newroot, folder)
        if not os.path.exists(newdir):
            print("Creating %s" % newdir)
            os.makedirs(newdir)

        files = folder_src_dst_map[folder]
        total_files_in_dir = len(files)
        print("Starting %s - %d files" % (newdir, total_files_in_dir))
        if not os.path.exists(newdir):
            print("Creating %s" % newdir)
            os.makedirs(newdir)

        logging.info("BEGINNING COPYING: %s" % newdir)
        for src_loc in files:
            sys.stdout.write("Copied %d/%d files %dMB, reduced by %dMB   \r" % (num_files, total_files_in_dir, total_size/1000000, compressed/1000000) )
            i+=1
            dest_loc=folder_src_dst_map[folder][src_loc]['dst']
            complete = folder_src_dst_map[folder][src_loc]['complete']
            if complete:
                num_files += 1
                continue

            file_name, file_extension = os.path.splitext(src_loc)
            im_type = file_name.split("_")[-1]
            if file_extension == ".tif":
                im_type = file_name.split("_")[-1]
                if im_type == "ir":
                    copy_file(src_loc, dest_loc)
                    logging.info("%d/%d COPIED IR: %s -> %s" % (num_files, total_files_in_dir, src_loc, dest_loc))
                    folder_src_dst_map[folder][src_loc]['complete'] = True
                elif im_type == "rgb":
                    subprocess.call("convert %s  -units PixelsPerInch -density 72x72 -quality 100 %s" % (src_loc, dest_loc),
                                    shell=True)
                    # im = Image.open(src_loc)
                    # im.save(dest_loc, quality=100)
                    old_size = os.path.getsize(src_loc)
                    new_size = os.path.getsize(dest_loc)
                    compressed += old_size - new_size
                    total_size += new_size
                    num_files += 1
                    folder_src_dst_map[folder][src_loc]['complete'] = True
                    logging.info("%d/%d COPIED EO: %s -> %s compressed %d bytes -> %d bytes" % (
                    num_files, total_files_in_dir, src_loc, dest_loc, old_size, new_size))
                else:
                    raise ("%s not ir or rgb" % file_name)
            else:
                copy_file(src_loc, dest_loc)
                logging.info("%d/%d COPIED OTHER: %s -> %s" % (num_files, total_files_in_dir, src_loc, dest_loc))
                folder_src_dst_map[folder][src_loc]['complete'] = True
            if i % 200 == 0:
                with open(pkl_file, "wb") as f:
                    pickle.dump(folder_src_dst_map, f)
        logging.info("FINISHED: %s" % (newdir))

    with open(pkl_file, "wb") as f:
        pickle.dump(folder_src_dst_map, f)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
print("DONE!")
