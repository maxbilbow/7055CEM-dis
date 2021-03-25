from pyspark.ml.feature import VectorAssembler

from pyspark.sql import DataFrame

VECTOR_COL="features"


class PreProcess:

    @staticmethod
    def create_training_and_test_data(dataset: DataFrame, columns: list = None):
        split = PreProcess.create_df_vector(dataset, columns).randomSplit([0.8, 0.2], 1234)
        train = split[0]
        test = split[1]

        print("Training: %s " % train.count())
        print("Test Data: %s " % test.count())
        return train, test

    @staticmethod
    def create_df_vector(df: DataFrame, columns: list = None):
        if columns is None:
            columns = PreProcess.get_feature_column_names(df)

        assembler = VectorAssembler(
            inputCols=columns,
            outputCol=VECTOR_COL)

        df_vector = assembler.transform(df)
        df_vector = df_vector.select(*[VECTOR_COL, "activity"])
        return df_vector

    @staticmethod
    def get_feature_column_names(df: DataFrame) -> list:
        return list(df.drop("activities").toPandas().columns)








