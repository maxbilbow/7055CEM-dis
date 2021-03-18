import pandas as pd
import os
import errno
from config import Config
from os.path import dirname
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression
from processing.pre_process import create_training_and_test_data

ROOT = os.path.join(dirname(__file__), "..", "..")
DATA_IN = os.path.join(ROOT, Config.get("path.data.in"))
PRE_PROCESSED = os.path.join(ROOT, Config.get("path.data.processed"))


def create_training_and_test_data(path="smartphone-activity", filename="dataset.csv", attributes="attributes.csv", keep_headers=True):
    source_data_path = os.path.join(
        DATA_IN,
        path,
        filename
    )
    attrs_data_path = os.path.join(
        DATA_IN,
        path,
        attributes
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

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example: Reading CSV file without mentioning schema") \
        .getOrCreate()

    dataset = spark.read.load(source_data_path, format="csv", sep=",", inferSchema=True, header=True)
    print("dataset loaded:")
    dataset.show(10)
    attrs = spark.read.csv(attrs_data_path, header=True)
    print("attributes loaded:")
    attrs.show()
    featureNames = []
    for singletonList in attrs.select("name").toPandas().values:
        for name in singletonList:
            if name.startswith("feature_"):
                featureNames.append(name)

    assembler = VectorAssembler(
        inputCols=featureNames,
        outputCol="features")

    training = assembler.transform(dataset)
    training.show()
    return dataset



def make_dirs(path):
    try:
        os.makedirs(dirname(path))
    except OSError as exc: # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise


create_training_and_test_data()