
import os

import mlflow
import pandas as pd

from botocore.exceptions import ClientError
from dateutil import parser
from datetime import datetime



def is_removed(status): return "removed" in status
def is_new_label(status): return "new" in status
def is_bad_res(status): return "bad_res" in status
def is_maybe_seal(status): return "maybe_seal" in status
def is_off_edge(status): return "off_edge" in status

def save_list_artifact(list, artifact_name, artifact_path):
    filename = '/tmp/%s' % artifact_name
    try:
        with open(filename, 'w') as f:
            for item in list:
                f.write("%s\n" % item)
        mlflow.log_artifact(filename, artifact_path)
    finally:
        os.remove(filename)

def parse_chess_filename (f):
    ct = len(os.path.basename(f).split('_'))
    info = {}
    e = f.split('_')
    e = [a for a in e if a != ""]
    if ct == 7:
        info['survey'] = e[0]
        info['flight'] = e[2]
        info['camPos'] = e[3]
        info['timestamp'] = e[4]
        info['camtype'] = e[5].split('-')[0]
    else:
        info['survey'] = e[0]
        info['flight'] = e[1]
        info['camPos'] = e[2]
        info['timestamp'] = e[3]
        info['camtype'] = e[4].split('-')[0]
    return info

def file_exists(path):
    if not pd.isna(path):
        return os.path.exists(path)
    return None

def find_image(dirs, name):
    for dir in dirs:
        im_path = os.path.join(dir, name).strip()
        if file_exists(im_path):
            return im_path
    return None

def parse_timestamp(ts_str):
    try:
        timestamp = datetime.strptime(ts_str, "%Y%m%d%H%M%S.%f%Z")
        timestamp_str = timestamp.strftime("%d-%m-%Y %H:%M:%S GMT-4")
        timestamp_obj = parser.parse(timestamp_str)
        return timestamp_obj
    except:
        return None

def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    percent = percent + "%% - %d/%d" % (iteration, total)
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s %s' % (prefix, bar, percent, suffix), end = printEnd)
    # Print New Line on Complete
    if iteration == total-1:
        print()


