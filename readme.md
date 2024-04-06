# Instructions

## Create the directories

```shell
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

## Up the airflow service

```shell
docker compose up airflow-init
```

## Up the service

```shell
docker-compose up
```

## External References

(Airflow Docker Documentation)[https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html]