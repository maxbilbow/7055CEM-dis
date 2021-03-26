import os

from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame

from pyspark.sql.functions import lit, monotonically_increasing_id, udf, format_string, concat
from repository.database import db
from config import Config
from processing.pre_process import PreProcess
from repository.MongoDB import MongoDb
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

DATA_IN = Config.get("path.data.in")
SOURCE_DATASET = os.path.join(DATA_IN, "dataset.csv")
MODELS_DIR = Config.get("path.data.models")
ATTRS_PATH = Config.get("path.data.attrs")
VECTOR_COL = "features"
LABEL_COL = "activity"


def get_filename(path: str) -> str:
    bits = path.split(os.path.sep)
    return bits[len(bits) - 1]


def hash_file(file_path: str):
    import hashlib

    hasher = hashlib.md5()
    with open(file_path, 'rb') as afile:
        buf = afile.read()
        hasher.update(buf)

    return hasher.hexdigest()


def append_column_identifiers(df: DataFrame, file_path: str) -> DataFrame:
    file_hash = hash_file(file_path)
    filename = get_filename(file_path)
    print("{} with hash: {}".format(filename, file_hash))
    return df \
        .withColumn("file", lit(filename)) \
        .withColumn("row", monotonically_increasing_id()) \
        .withColumn("_id", concat(lit(file_hash), lit("_"), monotonically_increasing_id()))


def create_schema():
    spark = db.create_spark_session()

    af = spark.read.load(ATTRS_PATH, format="csv", sep=",", inferSchema=True, header=True)
    af.head()

    def get_field(t: str):
        if t == "float":
            return FloatType()
        elif t == "integer":
            return IntegerType()
        else:
            raise Exception("Not expected: %s" % t)

    def to_struct(row) -> StructField:
        return StructField(row['name'], get_field(row['type']), nullable=False)

    struct_fields = af.rdd.map(to_struct).collect()

    return StructType(struct_fields)


def create_df_vector(df: DataFrame, columns: list = None):
    if columns is None:
        columns = PreProcess.get_feature_column_names(df)

    assembler = VectorAssembler(
        inputCols=columns,
        outputCol=VECTOR_COL)

    df_vector = assembler.transform(df)
    df_vector = df_vector.select(*[VECTOR_COL, LABEL_COL])
    return df_vector


def import_file(file_path: str):
    spark = db.create_spark_session()

    dataset = spark.read.load(file_path, format="csv", sep=",", schema=create_schema(), header=True)

    # Unable to fully prepare without spark mongo connector working
    dataset = create_df_vector(dataset)
    dataset = append_column_identifiers(dataset, file_path)
    dataset.printSchema()

    dataset.select(["file", "row", "activity"]).show(5)

    db.write_to_data_in(dataset)

    print("Entries: {}".format(MongoDb().data_in_table().count()))


def import_files(file_paths: list):
    for path in file_paths:
        import_file(path)


def start():
    import_files([SOURCE_DATASET])  # TODO Read all in folder


if __name__ == "__main__":
    start()
