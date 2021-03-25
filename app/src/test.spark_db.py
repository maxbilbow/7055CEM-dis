from repository.MongoDB import MongoDb
import repository.database as db
if __name__ == "__main__":
    sp = db.create_spark_session()
    df = sp.createDataFrame(["10","11","13"], "string").toDF("age")
    df.show()
    # table = MongoDb.test_table()
    db.write_to(df, "bums")
    print(MongoDb.table("bums").count())
