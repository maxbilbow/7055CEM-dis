import traceback

from pyspark.sql import DataFrame, SparkSession

from config import Config
from repository.MongoDB import MongoDb

database_name: str = Config.get("mongodb.database.name")

# TECH DEBT: unable to get spark mongo to play nice
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:1.1.0 import_data.py
SPARK_MONGO = False

spark_session: SparkSession = None


def create_spark_session():
    global spark_session
    if spark_session is not None:
        return spark_session

    credentials = Config.get("mongodb.credentials")
    port = Config.get("mongodb.port")

    if SPARK_MONGO:
        spark_session = SparkSession \
            .builder \
            .appName("myApp") \
            .config("spark.mongodb.input.uri", 'mongodb://{}:{}/{}'.format(credentials, port, database_name)) \
            .config("spark.mongodb.output.uri", 'mongodb://{}:{}/{}'.format(credentials, port, database_name)) \
            .getOrCreate()
    else:
        spark_session = SparkSession \
            .builder \
            .appName("myApp") \
            .getOrCreate()

    return spark_session


def write_to(df: DataFrame, collection_name: str):
    try:
        if SPARK_MONGO:
            df.write.format("com.mongodb.spark.sql.DefaultSource") \
                .mode("append") \
                .option("database", database_name) \
                .option("collection", collection_name) \
                .save()
        else:
            def write(row):
                item = row.asDict()
                collection = MongoDb().table(collection_name)
                collection.replace_one({"_id": item["_id"]}, item, upsert=True)

            df.rdd.foreach(write)

    except Exception as e:
        print("Failed to write to db")
        print(e)


def write_to_data_in(df: DataFrame):
    data_in_table_name: str = Config.get("mongodb.database.table.data_in")
    return write_to(df, data_in_table_name)


def write_to_results(df: DataFrame):
    results_table_name: str = Config.get("mongodb.database.table.results")
    return write_to(df, results_table_name)
