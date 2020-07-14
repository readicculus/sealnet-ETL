import os
import pickle
import subprocess

import numpy as np
import pandas as pd

# because multiple hotspots can be next to eachother in an image we want to split our train/test
# by image.  There are probably other reasons this is better too...
def test_train_split_by_image(dataset, train_ratio = .8):
    msk = np.random.rand(len(dataset)) < train_ratio

    train_images = dataset.images[msk]
    test_images = dataset.images[~msk]

    train = dataset.data[dataset.data.color_image.isin(train_images)]
    test = dataset.data[dataset.data.color_image.isin(test_images)]
    return train, test

def get_git_revisions_hash():
     return subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip()

def get_train_test_base(config):
    dataset_base = os.path.join(config.generated_data_base, config.dataset_path)
    train_base = os.path.join(dataset_base, "train")
    test_base = os.path.join(dataset_base, "test")
    return train_base, test_base

# get_train_test_list
def get_train_test_meta_data(config):
    dataset_base = os.path.join(config.generated_data_base, config.dataset_path)

    train_base = os.path.join(dataset_base, "train")
    test_base = os.path.join(dataset_base, "test")

    f_tr = open(os.path.join(train_base, "metadata.pickle"), 'rb')
    f_te = open(os.path.join(test_base, "metadata.pickle"), 'rb')
    train_meta = pickle.load(f_tr)
    test_meta = pickle.load(f_te)

    return train_meta, test_meta

class obj(object):
    def __init__(self, d):
        for a, b in d.items():
            if isinstance(b, (list, tuple)):
               setattr(self, a, [obj(x) if isinstance(x, dict) else x for x in b])
            else:
               setattr(self, a, obj(b) if isinstance(b, dict) else b)


import datetime


class Timer(object):
    def __init__(self, total):
        self.start = datetime.datetime.now()
        self.total = total

    def remains(self, done):
        now = datetime.datetime.now()
        # print(now-start)  # elapsed time
        left = (self.total - done) * (now - self.start) / done
        sec = int(left.total_seconds())
        if sec < 60:
            return "{} seconds".format(sec)
        else:
            return "{} minutes".format(int(sec / 60))
