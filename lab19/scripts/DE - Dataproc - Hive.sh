## Инициализация

# устанавливаем проект
gcloud projects list
gcloud config set project <my-project>

# выбираем зону для кластера, например, europe-west3-a
gcloud compute regions list
gcloud compute zones list

# устанавливаем переменные среды
export REGION=europe-west3
export ZONE=europe-west3-a
export PROJECT=$(gcloud info --format='value(config.project)')
export BUCKET_NAME=${PROJECT}-warehouse

# создаем бакет в google storage
gsutil mb -l ${REGION} gs://${BUCKET_NAME}
gsutil ls

# создадим инстанс MySQL для Hive Metastore
# может занять 3-5 минут
gcloud sql instances create hive-metastore-mysql \
    --database-version="MYSQL_5_7" \
    --activation-policy=ALWAYS \
    --zone $ZONE

gcloud sql instances list

## Создаем кластер Dataproc
gcloud config set compute/zone $ZONE

# Убедиться, что в строке --metadata в конце указано верное имя инстанса MySQL (предыдущий шаг)
# Можно создать кластер из одной или нескольких нод
# Для этого указать либо '--single-node' либо '--num-workers=2'
gcloud dataproc clusters create hive-cluster \
	--region=$REGION \
    --scopes cloud-platform \
    --image-version 1.3 \
    --bucket=$BUCKET_NAME \
	--master-machine-type=n1-standard-2 \
	--num-workers=4 \
	--worker-machine-type=n1-standard-4 \
	--optional-components=PRESTO \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/cloud-sql-proxy/cloud-sql-proxy.sh \
    --properties hive:hive.metastore.warehouse.dir=gs://${PROJECT}-warehouse/datasets \
    --metadata "hive-metastore-instance=${PROJECT}:${REGION}:hive-metastore-mysql"

# загрузить датасет
gsutil cp gs://hive-solution/part-00000.parquet \
	gs://${PROJECT}-warehouse/datasets/transactions/part-00000.parquet

gsutil ls -lr gs://${BUCKET_NAME}/datasets/

# создать external Hive table
gcloud dataproc jobs submit hive \
    --cluster hive-cluster \
    --execute "
      CREATE EXTERNAL TABLE transactions
      (SubmissionDate DATE, TransactionAmount DOUBLE, TransactionType STRING)
      STORED AS PARQUET
      LOCATION 'gs://${PROJECT}-warehouse/datasets/transactions';" \
      --region $REGION	

## запуск запросов с помощью jobs API:
gcloud dataproc jobs submit hive \
    --cluster hive-cluster \
    --execute "
      SELECT *
      FROM transactions
      LIMIT 10;" \
    --region $REGION \
    --quiet

## интерактивный режим в Hive CLI beeline
# подключимся к мастер-ноде
gcloud compute ssh hive-cluster-m
beeline -u "jdbc:hive2://hive-cluster-m:10000"

# запустим запросы 
!tables

!columns transactions

SELECT TransactionType, AVG(TransactionAmount) AS AverageAmount
FROM transactions
WHERE SubmissionDate = '2017-12-22'
GROUP BY TransactionType;

# выйдем из beeline
!quit

## запустим запросы с помощью pyspark
pyspark

# контекст и запрос
from pyspark.sql import HiveContext
hc = HiveContext(sc)
hc.sql("""
SELECT SubmissionDate, AVG(TransactionAmount) as AvgDebit
FROM transactions
WHERE TransactionType = 'debit'
GROUP BY SubmissionDate
HAVING SubmissionDate >= '2017-10-01' AND SubmissionDate < '2017-10-06'
ORDER BY SubmissionDate
""").show()

exit()

# покинем сессию ssh и мастер-ноду
exit

## Выгрузим второй датасет из BigQuery - chicago_taxi_trips
bq --location=us extract --destination_format=CSV \
     --field_delimiter=',' --print_header=false \
       "bigquery-public-data:chicago_taxi_trips.taxi_trips" \
       gs://${BUCKET_NAME}/chicago_taxi_trips/csv/shard-*.csv

# посмотрим на список выгруженных файлов (72 GiB)
gsutil ls -rl gs://${BUCKET_NAME}/chicago_taxi_trips/csv/

# удалим часть файлов для ускорения работы
gsutil ls gs://${BUCKET_NAME}/chicago_taxi_trips/csv/ | head -261 | xargs gsutil rm # для single-node head -281
gsutil ls -rl gs://${BUCKET_NAME}/chicago_taxi_trips/csv/ # 7.71 GiB

# Создадим Hive external table для chicago_taxi_trips_csv
gcloud dataproc jobs submit hive \
    --cluster hive-cluster \
    --region=${REGION} \
    --execute "
        CREATE EXTERNAL TABLE chicago_taxi_trips_csv(
          unique_key   STRING,
          taxi_id  STRING,
          trip_start_timestamp  STRING,
          trip_end_timestamp  STRING,
          trip_seconds  INT,
          trip_miles   FLOAT,
          pickup_census_tract  INT,
          dropoff_census_tract  INT,
          pickup_community_area  INT,
          dropoff_community_area  INT,
          fare  FLOAT,
          tips  FLOAT,
          tolls  FLOAT,
          extras  FLOAT,
          trip_total  FLOAT,
          payment_type  STRING,
          company  STRING,
          pickup_latitude  FLOAT,
          pickup_longitude  FLOAT,
          pickup_location  STRING,
          dropoff_latitude  FLOAT,
          dropoff_longitude  FLOAT,
          dropoff_location  STRING)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        location 'gs://${BUCKET_NAME}/chicago_taxi_trips/csv/';"

# убедимся в создании таблицы и ее работе (20 млн записей)
gcloud dataproc jobs submit hive \
    --cluster hive-cluster \
    --region=${REGION} \
    --execute "SELECT COUNT(*) FROM chicago_taxi_trips_csv;"

# создадим таблицу в формате parquet
gcloud dataproc jobs submit hive \
    --cluster hive-cluster \
    --region=${REGION} \
    --execute "
        CREATE EXTERNAL TABLE chicago_taxi_trips_parquet(
          unique_key   STRING,
          taxi_id  STRING,
          trip_start_timestamp  STRING,
          trip_end_timestamp  STRING,
          trip_seconds  INT,
          trip_miles   FLOAT,
          pickup_census_tract  INT,
          dropoff_census_tract  INT,
          pickup_community_area  INT,
          dropoff_community_area  INT,
          fare  FLOAT,
          tips  FLOAT,
          tolls  FLOAT,
          extras  FLOAT,
          trip_total  FLOAT,
          payment_type  STRING,
          company  STRING,
          pickup_latitude  FLOAT,
          pickup_longitude  FLOAT,
          pickup_location  STRING,
          dropoff_latitude  FLOAT,
          dropoff_longitude  FLOAT,
          dropoff_location  STRING)
        STORED AS PARQUET
        location 'gs://${BUCKET_NAME}/chicago_taxi_trips/parquet/';"

# запишем данные в таблицу формата parquet (примерно 7 минут)
gcloud dataproc jobs submit hive \
    --cluster hive-cluster \
    --region=${REGION} \
    --execute "
        INSERT OVERWRITE TABLE chicago_taxi_trips_parquet
        SELECT * FROM chicago_taxi_trips_csv;"

# убедимся, что там такое же количество строк
gcloud dataproc jobs submit hive \
    --cluster hive-cluster \
    --region=${REGION} \
    --execute "SELECT COUNT(*) FROM chicago_taxi_trips_parquet;"

# ssh 
gcloud compute ssh hive-cluster-m

# на этот раз используем движок presto для sql-запросов
presto --catalog hive --schema default
show tables;

# запустим запросы. Эти же запросы можно запускать в Hive CLI (Beeling)
select count(*) from chicago_taxi_trips_csv where trip_miles > 50; # 0:42 [20.7M rows, 7.71GB] [490K rows/s, 187MB/s]
select count(*) from chicago_taxi_trips_parquet where trip_miles > 50; # 0:11 [20.7M rows, 52.2MB] [1.89M rows/s, 4.76MB/s]

select distinct company, payment_type from chicago_taxi_trips_csv limit 10; # 0:02 [228K rows, 87.5MB] [137K rows/s, 52.3MB/s]
select distinct company, payment_type from chicago_taxi_trips_parquet limit 10; # 0:01 [8.19K rows, 1.93MB] [6.21K rows/s, 1.46MB/s]

select company, payment_type, avg(fare), count(tips), sum(trip_total) 
from chicago_taxi_trips_csv where trip_miles > 10 
group by 1, 2 order by 5 desc limit 10 ; # 0:40 [20.7M rows, 7.71GB] [519K rows/s, 198MB/s]

select company, payment_type, avg(fare), count(tips), sum(trip_total) 
from chicago_taxi_trips_parquet where trip_miles > 10 
group by 1, 2 order by 5 desc limit 10 ; # 0:10 [20.7M rows, 154MB] [2M rows/s, 14.9MB/s]

# покинуть сессию presto
exit

## Посмотрим внутрь Hive Metastore
gcloud sql connect hive-metastore-mysql --user=root # без пароля (ENTER)

SHOW DATABASES;
USE hive_metastore;

SHOW TABLES;

# bucket location в metastore
SELECT DB_LOCATION_URI FROM DBS;

# список таблиц 
SELECT TBL_NAME, TBL_TYPE FROM TBLS;

# список колонок таблицы
SELECT COLUMN_NAME, TYPE_NAME
FROM COLUMNS_V2 c, TBLS t
WHERE c.CD_ID = t.SD_ID AND t.TBL_NAME = 'chicago_taxi_trips_parquet';

# формат файла и расположение
SELECT INPUT_FORMAT, LOCATION
FROM SDS s, TBLS t
WHERE s.SD_ID = t.SD_ID ;

# покинуть сессию mysql
exit

## Освободить ресурсы, удалить инстансы и виртуалки 
gcloud dataproc clusters delete hive-cluster --region=$REGION --quiet
gcloud sql instances delete hive-metastore-mysql --quiet
gsutil rm -r gs://${BUCKET_NAME}