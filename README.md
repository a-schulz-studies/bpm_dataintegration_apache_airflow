## Airflow Docs:

https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html


## Initial setup:
```bash
docker compose up airflow-init
```

## Starting the services
```bash
docker compose up
```

## Debugging / Testing DAG locally
```bash
docker compose up airflow-python
```

Pycharm -> Interpreter Settings -> Add Interpreter -> Docker Compose -> service=aiflow-python

Run Configuration -> Commands and Options -> `exec`