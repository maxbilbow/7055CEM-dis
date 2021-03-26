## Data Intensive Systems Coursework Project

Data set used: [Smartphone Activity Study](https://www.apispreadsheets.com/datasets/122)

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
sh start --import
```

### Train the model
```shell
sh start --train
```

### Make predictions 
```shell
sh start --predict
```

### Evaluate predictions 
(assuming labels are available)
```shell
sh start --evaluate
```


### Run everything in one go
(assuming labels are available)
```shell
sh start -itpe
```


## Run Jupyter Notebook

The [load_lrm.ipynb](app/work/load_lrm.ipynb) notebook will verify the saved model against whatever data you provide it.
1. Get the browser token
```shell
docker exec -it dis_jupyter /bin/bash
## (base) jovyan@3d3b4630e77a:~$
jupyter notebook list
# Copy the token from:
# http://0.0.0.0:8888/?token={token}:
```

2. Enter into browser: `http://localhost:8888/?token={token}`

3. Open the [work/load_lrm.ipynb](app/work/load_lrm.ipynb) notebook

4. Select "Run all cells"
