import os

from pyspark.sql import DataFrame

from pyspark.sql.functions import lit, monotonically_increasing_id, udf, format_string, concat
import repository.database as db
from repository.MongoDB import MongoDb

ROOT = r".."
DATA_IN = os.path.join(ROOT, "data")  # Config.get("path.data.in"))
DATA_FOLDER = "meta"
SOURCE_DATASET = os.path.join(DATA_IN, DATA_FOLDER, "dataset.csv")


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


def import_file(file_path: str):
    spark = db.create_spark_session()

    dataset = spark.read.load(file_path, format="csv", sep=",", inferSchema=True, header=True)

    # Unable to fully prepare without spark mongo connector working
    # dataset = PreProcess.create_df_vector(dataset)
    dataset = append_column_identifiers(dataset, file_path)

    dataset.select(["file", "row", "activity"]).show(5)

    db.write_to_data_in(dataset)

    print("Entries: {}".format(MongoDb().data_in_table().count()))


def import_files(file_paths: list):
    for path in file_paths:
        import_file(path)


if __name__ == "__main__":
    import_files([SOURCE_DATASET])
