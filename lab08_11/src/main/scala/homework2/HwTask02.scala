package homework2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/*
Задание 2.
Загрузить данные в RDD из файла с фактическими данными поездок в Parquet
(src/main/resources/data/yellow_taxi_jan_25_2018).
С помощью lambda построить таблицу, которая покажет В какое время происходит больше всего вызовов.
Результат вывести на экран и в txt файл c пробелами.
*/

object HwTask02 extends App{

  case class TaxiRide(
                       VendorID: Int,
                       tpep_pickup_datetime: String,
                       tpep_dropoff_datetime: String,
                       passenger_count: Int,
                       trip_distance: Double,
                       RatecodeID: Int,
                       store_and_fwd_flag: String,
                       PULocationID: Int,
                       DOLocationID: Int,
                       payment_type: Int,
                       fare_amount: Double,
                       extra: Double,
                       mta_tax: Double,
                       tip_amount: Double,
                       tolls_amount: Double,
                       improvement_surcharge: Double,
                       total_amount: Double
                     )

  val spark = SparkSession.builder()
    .appName("Task08_02")
    .config("spark.master", "local")
    .config("spark.driver.host","localhost")
    .getOrCreate()

  import spark.implicits._

  val taxiFactsDF: DataFrame = spark
    .read
    .parquet("src/main/resources/data/yellow_taxi_jan_25_2018")

  val taxiFactsDS: Dataset[TaxiRide] =
    taxiFactsDF
      .as[TaxiRide]

  val taxiFactsRDD: RDD[TaxiRide] =
    taxiFactsDS.rdd

  val form = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  val TopTimeRDD = taxiFactsRDD
    .map(row => (LocalDateTime.parse(row.tpep_pickup_datetime, form).toLocalTime.getHour, 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, false)

  TopTimeRDD.take(30).foreach(println)

  TopTimeRDD
    .map(row =>row.productIterator.mkString("\t"))
    .coalesce(1)
    .saveAsTextFile("src/main/resources/data/out/top_time")
}
