[Based on](https://github.com/bitnami/bitnami-docker-spark)

```shell
docker-compose up -d
docker ps # get instance names
```

## 1. Train the Data

### Set up the environment
```shell
docker exec -it dis_spark /bin/bash
## In the container:
cd /app/src
sh setup.sh
```
### Import Data into MongoDB
```shell
spark-submit import.py
```
### Train the data
```shell
spark-submit train_logical_regression.py
```

## Run Jupyter Notebook

```shell
docker exec -it dis_spark /bin/bash
## (base) jovyan@3d3b4630e77a:~$
jupyter notebook list
# Copy the token from:
# http://0.0.0.0:8888/?token={token}:
```

Enter into browser: `http://localhost:8888/?token={token}`
