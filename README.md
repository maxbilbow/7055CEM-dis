[Based on](https://github.com/bitnami/bitnami-docker-spark)

```shell
docker pull bitnami/spark
docker-compose up -d
docker ps # get instance names
docker exec -it instance_name /bin/bash
```

```shell
cd /app
virtualenv ve -p python3
source ve/bin/activate
pip install -r requirements
```