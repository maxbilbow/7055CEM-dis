from processing.feature_reduction import find_correlation
from pyspark.ml.feature import VectorAssembler
import numpy as np

from pyspark.sql import SparkSession, DataFrame

VECTOR_COL="features"

class PreProcess:
    @staticmethod
    def create_training_and_test_data(spark: SparkSession, dataset: DataFrame):
        df = create_df_vector(dataset).toPandas()
        msk = np.random.rand(len(df)) < 0.8

        train = spark.createDataFrame(df[msk])

        test = spark.createDataFrame(df[~msk])

        print("Training: %s " % train.count())
        print("Test Data: %s " % test.count())
        return train, test


def reduce_feature_set(df: DataFrame):
    pandas_df = df.toPandas()
    print("All Features: %s" % len(pandas_df.columns))
    reduced_feature_set = find_correlation(pandas_df)
    print("Reduced Features: %s" % len(reduced_feature_set))
    return list(reduced_feature_set)


def create_df_vector(df: DataFrame):
    columns: list = reduce_feature_set(df)
    assembler = VectorAssembler(
        inputCols=columns,
        outputCol=VECTOR_COL)

    df_vector = assembler.transform(df)
    df_vector = df_vector.select(*[VECTOR_COL, "activity"])
    return df_vector




