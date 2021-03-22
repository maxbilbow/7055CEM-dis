from pyspark.sql import DataFrame


def get_accuracy(prediction: DataFrame):
    result = prediction.select("features", "activity", "probability", "prediction") \
        .collect()

    right = wrong = 0
    for row in result:
        if row.activity == row.prediction:
            right += 1
        else:
            wrong += 1

    total = right + wrong
    percent = round(right * 100 / total)
    print("Accuracy: %s/%s (%s%%)" % (right, total, percent))

    # for row in result:
    #     print("activity=%s -> prob=%s, prediction=%s" % (row.activity, row.myProbability, row.prediction))

    return percent
