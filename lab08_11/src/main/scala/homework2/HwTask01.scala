package homework2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col}

/*
Задание 1.
Загрузить данные в первый DataFrame из файла с фактическими данными поездок в Parquet
(src/main/resources/data/yellow_taxi_jan_25_2018). Загрузить данные во второй DataFrame
из файла со справочными данными поездок в csv (src/main/resources/data/taxi_zones.csv).
С помощью DSL построить таблицу, которая покажет какие районы самые популярные для заказов.
Результат вывести на экран и записать в файл Паркет.
*/

object HwTask01 extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .config("spark.driver.host","localhost")
    .getOrCreate()

  val taxiFactsDF = spark.read.parquet("src/main/resources/data/yellow_taxi_jan_25_2018")

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  val TopBoroughDF = taxiFactsDF
    .join(broadcast(taxiZonesDF), col("DOLocationID") === col("LocationID")
      || col("PULocationID") === col("LocationID"), "left")
    .groupBy(col("Borough"))
    .count()
    .orderBy(col("count").desc)

  TopBoroughDF
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet("src/main/resources/data/out/top_borough")

  TopBoroughDF.show()

}
