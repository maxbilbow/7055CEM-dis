from repository.MongoDB import MongoDb

if __name__ == "__main__":
    table = MongoDb.test_table()
    print(table.insert_one({"test": "bob"}).inserted_id)
    print(MongoDb.test_table().find_one())
