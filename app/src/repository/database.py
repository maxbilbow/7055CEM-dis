import json
import traceback
from abc import abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from config import Config
from repository.MongoDB import MongoDb

database_name: str = Config.get("database.name")

# TECH DEBT: unable to get spark mongo to play nice
# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:1.1.0 import_data.py
SPARK_MONGO = False


class Db:
    spark_session: SparkSession = None
    data_in_table_name: str = Config.get("database.table.data_in")
    results_table_name: str = Config.get("database.table.results")

    @abstractmethod
    def write_to(self, df: DataFrame, collection_name):
        pass

    @abstractmethod
    def load_data_from(self, collection_name: str) -> DataFrame:
        pass

    def create_spark_session(self):
        if Db.spark_session is not None:
            return Db.spark_session

        Db.spark_session = self._create_spark_session()
        return Db.spark_session

    def _create_spark_session(self):
        return SparkSession \
            .builder \
            .appName("myApp") \
            .getOrCreate()

    def write_to_data_in(self, df: DataFrame):
        return self.write_to(df, Db.data_in_table_name)

    def write_to_results(self, df: DataFrame):
        return self.write_to(df, Db.results_table_name)

    def load_data_in(self) -> DataFrame:
        return self.load_data_from(Db.data_in_table_name)

    def load_results(self) -> DataFrame:
        return self.load_data_from(Db.results_table_name)


class SimpleMongoDb(Db):
    def write_to(self, df: DataFrame, collection_name: str):
        try:
            def write(row):
                item = row.asDict()
                collection = MongoDb().table(collection_name)
                collection.replace_one({"_id": item["_id"]}, item, upsert=True)

            df.rdd.foreach(write)
        except Exception as e:
            print("Failed to write to db")
            print(e)


class SparkMongo(Db):
    def _create_spark_session(self):
        credentials = Config.get("database.mongodb.credentials")
        port = Config.get("database.mongodb.port")
        return SparkSession \
            .builder \
            .appName("myApp") \
            .config("spark.mongodb.input.uri", 'mongodb://{}:{}/{}'.format(credentials, port, database_name)) \
            .config("spark.mongodb.output.uri", 'mongodb://{}:{}/{}'.format(credentials, port, database_name)) \
            .getOrCreate()

    def write_to(self, df: DataFrame, collection_name: str):
        df.write.format("com.mongodb.spark.sql.DefaultSource") \
            .mode("append") \
            .option("database", database_name) \
            .option("collection", collection_name) \
            .save()


class TextDb(Db):
    _format = Config.get("database.textdb.format")
    _path = Config.get("database.textdb.path")

    def write_to(self, df: DataFrame, collection_name):
        df.write.format(TextDb._format) \
            .mode("overwrite") \
            .save("{}/{}".format(TextDb._path, collection_name))

        with open("{}/{}_schema.json".format(TextDb._path, collection_name), "w") as f:
            json.dump(df.schema.jsonValue(), f)

    def load_data_from(self, collection_name: str) -> DataFrame:
        path = "{}/{}".format(TextDb._path, collection_name)
        print("Reading: {}".format(path))
        with open("{}/{}_schema.json".format(TextDb._path, collection_name)) as f:
            schema = StructType.fromJson(json.load(f))
            print(schema.simpleString())
            return self.create_spark_session().read.format(TextDb._format) \
                .load(path, schema=schema)


db = TextDb()
