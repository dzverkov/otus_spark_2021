package homework11

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import RddTypes.{TripDist, TripStat}
import homework11.DataUtils.readParquet

/*
Задание 3.
Загрузить данные в DataSet из файла с фактическими данными поездок в Parquet
(src/main/resources/data/yellow_taxi_jan_25_2018).
С помощью DSL и lambda построить таблицу, которая покажет. Как происходит распределение поездок по дистанции?
Результат вывести на экран и записать в бд Постгрес (докер в проекте).
Для записи в базу данных необходимо продумать и также приложить инит sql файл со структурой.
*/

object HwTask03 extends App{

  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"

  implicit val spark = SparkSession.builder()
    .appName("Task11_03")
    .config("spark.master", "local")
    .config("spark.driver.host","localhost")
    .getOrCreate()

  def loadData(path: String)(implicit spark: SparkSession):Dataset[TripDist] = {
    import spark.implicits._

    readParquet(path)
      .select(col("trip_distance"))
      .as[TripDist]
  }

  def getTripStat(ds: Dataset[TripDist])(implicit spark: SparkSession):Dataset[TripStat] = {
    import spark.implicits._

    ds
      .filter(col("trip_distance") > lit(0))
      .select(
        count(col("trip_distance")).alias("total"),
        max(col("trip_distance")).alias("max_dist"),
        min(col("trip_distance")).alias("min_dist"),
        round(avg(col("trip_distance")), 0).alias("avg_dist"),
        round(stddev_pop(col("trip_distance")), 2).alias("std")
      )
      .as[TripStat]
  }

  def saveData(ds: Dataset[TripStat]):Unit = {
    ds
      .write
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "trip_stat")
      .option("user", user)
      .option("password", password)
      .mode("overwrite")
      .save
  }

  val distDS: Dataset[TripDist] = loadData("src/main/resources/data/yellow_taxi_jan_25_2018")
  val resDS: Dataset[TripStat] = getTripStat(distDS)

  resDS.show()

  saveData(resDS)

}
