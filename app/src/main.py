#!/usr/bin/python
from getopt import getopt, GetoptError

from tasks import train_lrm, make_predictions, import_data, evaluate_predictions
import sys


class Tasks:
    import_data = False
    train_data = False
    predict = False
    eval = False


def run_tasks():
    if Tasks.import_data:
        print("Importing and preparing data...")
        import_data.start()
    if Tasks.train_data:
        print("Training Model...")
        train_lrm.start()
    if Tasks.predict:
        print("Making predictions based on saved model")
        make_predictions.start()
    if Tasks.eval:
        print("Evaluating predictions if labels are available")
        evaluate_predictions.start()


def show_help():
    print("""
    -i      --import
    -t      --train
    -p      --predict
    -e      --eval
    """)


def main(argv):
    try:
        opts, args = getopt(argv, "hitpe", ["import", "train", "predict", "eval", "evaluate"])
    except GetoptError:
        print("Commands not recognised: {}".format(argv))
        show_help()
        sys.exit(2)

    for opt, arg in opts:
        print(opt)
        if opt == "-h":
            show_help()
            sys.exit()
        if opt in ("-i", "--import"):
            Tasks.import_data = True
        if opt in ("-t", "--train"):
            Tasks.train_data = True
        if opt in ("-p", "--predict"):
            Tasks.predict = True
        if opt in ("-e", "--eval", "--evaluate"):
            Tasks.eval = True

    run_tasks()


if __name__ == "__main__":
    main(sys.argv[1:])
