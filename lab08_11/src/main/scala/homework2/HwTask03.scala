package homework2

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

/*
Задание 3.
Загрузить данные в DataSet из файла с фактическими данными поездок в Parquet
(src/main/resources/data/yellow_taxi_jan_25_2018).
С помощью DSL и lambda построить таблицу, которая покажет. Как происходит распределение поездок по дистанции?
Результат вывести на экран и записать в бд Постгрес (докер в проекте).
Для записи в базу данных необходимо продумать и также приложить инит sql файл со структурой.
*/

object HwTask03 extends App{

  case class TripDist(trip_distance: Double)
  case class Stat(
                   total: Long,
                   max_dist: Double,
                   min_dist: Double,
                   avg_dist: Double,
                   std: Double
                 )

  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"

  val spark = SparkSession.builder()
    .appName("Task08_03")
    .config("spark.master", "local")
    .config("spark.driver.host","localhost")
    .getOrCreate()

  import spark.implicits._

  val distDS: Dataset[TripDist] = spark
    .read
    .parquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    .select(col("trip_distance"))
    .as[TripDist]

  val resDS: Dataset[Stat] = distDS
    .filter(col("trip_distance") > lit(0))
    .select(
      count(col("trip_distance")).alias("total"),
      max(col("trip_distance")).alias("max_dist"),
      min(col("trip_distance")).alias("min_dist"),
      round(avg(col("trip_distance")), 0).alias("avg_dist"),
      round(stddev_pop(col("trip_distance")), 2).alias("std")

    )
    .as[Stat]

  resDS.show()

  resDS.write
    .format("jdbc")
    .option("url", url)
    .option("dbtable", "trip_stat")
    .option("user", user)
    .option("password", password)
    .mode("overwrite")
    .save

}
