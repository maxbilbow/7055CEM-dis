from pymongo import MongoClient
from config import Config

database_name: str = Config.get("mongodb.database.name")
data_in_table_name: str = Config.get("mongodb.database.table.data_in")
results_table_name: str = Config.get("mongodb.database.table.results")
credentials = Config.get("mongodb.credentials")
port = Config.get("mongodb.port")


class MongoDb:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            print('Creating the database')
            cls._instance = super(MongoDb, cls).__new__(cls)
            cls._database_name = database_name
            print("Connecting to database...")
            cls._cluster = MongoClient('mongodb://{}'.format(credentials), port)
            cls._db = cls._cluster[database_name]
        return cls._instance

    @staticmethod
    def data_in_table():
        return MongoDb()._db[data_in_table_name]

    @staticmethod
    def results_table():
        return MongoDb()._db[results_table_name]

    @staticmethod
    def test_table():
        return MongoDb()._db["test"]

    @staticmethod
    def table(table_name: str):
        return MongoDb()._db[table_name]


if __name__ == "__main__":
    table = MongoDb.test_table()
    print(table.insert_one({"test": "bob"}).inserted_id)
    table = MongoDb.test_table()
