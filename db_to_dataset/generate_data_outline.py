import argparse
import os
import pickle

from db_to_dataset.RemoteDataloader import RemoteChipsDataset

DATA_BASE = "/fast/generated_data/"
# Generate the outline of a dataset.  What does this mean?
# Essentially its nicer to break this up into two steps.  In this step we create the new folder in which
# the new dataset will reside, we create a test/train split based on the config, and we pickle the config file.
# The next step is generate_data_items which actually does the chipping based on the outlined files and config
# that we generate here.
parser = argparse.ArgumentParser(description='Generate new dataset outline.')
parser.add_argument('--split', default=.8, help='Train/Test split. ex ".8" means 80% train 20% test', type=float)

args = parser.parse_args()

TEST_TRAIN_SPLIT = args.split
name = "dataset-832"
datasets = [f for f in os.listdir(DATA_BASE) if not "." in f and name in f]
datasets = [f for f in datasets if name in f]
dataset_id = len(datasets)
dataset_folder = name+"_%d"%dataset_id
dataset_base = os.path.join(DATA_BASE, dataset_folder)


if not os.path.exists(dataset_base):
    os.makedirs(dataset_base)
else:
    raise Exception('directory %s already exists' % dataset_base)


train_dataset = RemoteChipsDataset(chip_width=832, chip_height=832)
test_dataset = RemoteChipsDataset(chip_width=832, chip_height=832)
train_dataset.save(os.path.join(dataset_base, "train.pkl"))
test_dataset.save(os.path.join(dataset_base, "test.pkl"))

print("Created new dataset outline in: %s" % dataset_base)


