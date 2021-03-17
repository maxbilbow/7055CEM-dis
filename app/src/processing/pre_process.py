import pandas as pd
import os

from config import Config
from os.path import dirname
DATA_IN = r"%s/../../%s" % (dirname(__file__), Config.get("path.data.in"))
PRE_PROCESSED = r"%s/../../%s" % (dirname(__file__), Config.get("path.data.processed"))
print(DATA_IN)

def create_training_and_test_data():
    af = pd.read_csv("%s/smartphone-activity/attributes.csv" % DATA_IN)
    type(af)
    df = pd.read_csv("%s/smartphone-activity/dataset.csv" % DATA_IN)
    type(df)
    print(df.shape)
    split()



def split(output_path=PRE_PROCESSED, keep_headers=True):
    source_data_path = os.path.join(
        DATA_IN,
        "smartphone-activity",
        "dataset.csv"
    )
    training_data_path = os.path.join(
        output_path,
        "dataset_train.csv"
    )
    testing_data_path = os.path.join(
        output_path,
        "dataset_test.csv"
    )

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