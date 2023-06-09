# Readme

Florian GUSTIN - Haitem LIHAM and Cedric LEPROHON

## Deployment

You must docker and override envirnonment ``SPARK_WORKER_MEMORY`` (default 4 gb) in ```docker-compose.yml```

```sh
docker compose up
```

You need intialize the data, go to into your container and execute this line

```sh
/spark/bin/spark-submit /app/init.py
```

## Run
```sh
/spark/bin/spark-submit /app/steps.py
```