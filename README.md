# Building-data-pipeline

```bash
project-structure/
├── docker-compose.yml
├── .env
├── airflow/
│   └── dags/
├── spark/
│   ├── app/
│   └── jars/
├── kafka/
│   └── topics/
├── data_lake/
│   ├── raw/
│   └── processed/
```

.env
```bash
MYSQL_ROOT_PASSWORD=root
MYSQL_DATABASE=streamdb
```



Kafka: localhost:9092

Spark UI: localhost:8080

MongoDB: localhost:27017

MySQL: localhost:3306

Airflow UI: localhost:8089

