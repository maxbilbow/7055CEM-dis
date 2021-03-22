from processing.feature_reduction import get_reduced_columns
from pyspark.ml.feature import VectorAssembler
import numpy as np

from pyspark.sql import SparkSession, DataFrame

VECTOR_COL="features"


class PreProcess:

    @staticmethod
    def create_training_and_test_data(spark: SparkSession, dataset: DataFrame, columns: list = None):
        df = PreProcess.create_df_vector(dataset, columns).toPandas()
        msk = np.random.rand(len(df)) < 0.8

        train = spark.createDataFrame(df[msk])

        test = spark.createDataFrame(df[~msk])

        print("Training: %s " % train.count())
        print("Test Data: %s " % test.count())
        return train, test

    @staticmethod
    def create_df_vector(df: DataFrame, columns: list):
        if columns is None:
            columns = list(df.drop("activities").toPandas().columns)

        assembler = VectorAssembler(
            inputCols=columns,
            outputCol=VECTOR_COL)

        df_vector = assembler.transform(df)
        df_vector = df_vector.select(*[VECTOR_COL, "activity"])
        return df_vector







