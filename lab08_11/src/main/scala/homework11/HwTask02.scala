package homework11

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.time.LocalDateTime
import RddTypes.{DT_FRMT, TaxiRide}
import homework11.DataUtils.readParquet

/*
Задание 2.
Загрузить данные в RDD из файла с фактическими данными поездок в Parquet
(src/main/resources/data/yellow_taxi_jan_25_2018).
С помощью lambda построить таблицу, которая покажет В какое время происходит больше всего вызовов.
Результат вывести на экран и в txt файл c пробелами.
*/

object HwTask02 extends App {

  implicit val spark = SparkSession.builder()
    .appName("Task11_02")
    .config("spark.master", "local")
    .config("spark.driver.host","localhost")
    .getOrCreate()

  def loadData(path: String)(implicit spark: SparkSession): RDD[TaxiRide] = {
    import spark.implicits._

    val taxiFactsDF: DataFrame = readParquet(path)

    val taxiFactsDS: Dataset[TaxiRide] =
      taxiFactsDF
        .as[TaxiRide]

      taxiFactsDS.rdd
  }

  def getTopTime(rdd: RDD[TaxiRide]): RDD[(Int, Int)] = {
    rdd
      .map(row => (LocalDateTime.parse(row.tpep_pickup_datetime, DT_FRMT).toLocalTime.getHour, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
  }

  def saveData(rdd: RDD[(Int, Int)], path: String): Unit = {
    rdd
      .map(row =>row.productIterator.mkString("\t"))
      .coalesce(1)
      .saveAsTextFile(path)
  }

  val taxiFactsRDD: RDD[TaxiRide] = loadData("src/main/resources/data/yellow_taxi_jan_25_2018")
  val TopTimeRDD = getTopTime(taxiFactsRDD)

  TopTimeRDD.take(30).foreach(println)

  saveData(TopTimeRDD, "src/main/resources/data/out/top_time")

}
