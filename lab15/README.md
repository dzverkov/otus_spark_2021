
1. Создать топики
```bash
docker exec kafka_scala_example_broker_1 kafka-topics \
  --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic input

docker exec kafka_scala_example_broker_1 kafka-topics \
  --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic prediction
```

2. Загрузить данные в топик `input` (host - Windows)
```bash
docker cp .\src\main\resources\data\iris.csv kafka_scala_example_broker_1:/home
docker exec kafka_scala_example_broker_1 bash -c "awk -F ',' 'NR > 1 { print $1 "," $2 "," $3 "," $4 }' < /home/iris.csv | kafka-console-producer --topic input --bootstrap-server localhost:9092"
```

* Просмотр топиков
```bash
docker exec kafka_scala_example_broker_1 kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic input \
--offset earliest \ 
--partition 0

docker exec kafka_scala_example_broker_1 kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic prediction
```
