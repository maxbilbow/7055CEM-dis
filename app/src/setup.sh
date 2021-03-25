#!/usr/bin/env bash

cp /app/src/conf/log4j.properties /opt/bitnami/spark/conf/log4j.properties && echo "Log4J properties updated"

pip install -r requirements.txt && echo "Requirements Installed"

#spark-submit train_logical_regression.py && "Training complete"
