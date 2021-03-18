import pandas as pd
import os
import errno
from config import Config
from os.path import dirname

ROOT = os.path.join(dirname(__file__), "..", "..")
DATA_IN = os.path.join(ROOT, Config.get("path.data.in"))
PRE_PROCESSED = os.path.join(ROOT, Config.get("path.data.processed"))


def create_training_and_test_data():
    split()


def make_dirs(path):
    try:
        os.makedirs(dirname(path))
    except OSError as exc: # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise


def split(path="smartphone-activity", filename="dataset.csv", keep_headers=True):
    source_data_path = os.path.join(
        DATA_IN,
        path,
        filename
    )
    training_data_path = os.path.join(
        PRE_PROCESSED,
        path,
        "train-%s" % filename
    )
    testing_data_path = os.path.join(
        PRE_PROCESSED,
        path,
        "test-%s" % filename
    )
    make_dirs(training_data_path)

    with open(source_data_path) as dataset:
        with open(training_data_path, 'x') as train:
            with open(testing_data_path, 'x') as test:
                writeToTrain = True
                first = True
                for line in dataset:
                    if first and keep_headers:
                        train.writelines([line])
                        test.writelines([line])
                        first = False
                        continue

                    output = train if writeToTrain else test
                    writeToTrain = not writeToTrain
                    output.writelines([line])


create_training_and_test_data()


def get_training_and_test_data():
    split()
