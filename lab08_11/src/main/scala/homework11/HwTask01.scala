package homework11

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col}
import DataUtils.{readCSV, readParquet}

/*
Задание 1.
Загрузить данные в первый DataFrame из файла с фактическими данными поездок в Parquet
(src/main/resources/data/yellow_taxi_jan_25_2018). Загрузить данные во второй DataFrame
из файла со справочными данными поездок в csv (src/main/resources/data/taxi_zones.csv).
С помощью DSL построить таблицу, которая покажет какие районы самые популярные для заказов.
Результат вывести на экран и записать в файл Паркет.
*/

object HwTask01 extends App {

  implicit val spark = SparkSession.builder()
    .appName("Task11_01")
    .config("spark.master", "local")
    .config("spark.driver.host","localhost")
    .getOrCreate()

  def getTopBorough(pTaxiFactsDF: DataFrame, pTaxiZonesDF: DataFrame): DataFrame = {
    pTaxiFactsDF
      .join(broadcast(pTaxiZonesDF), col("DOLocationID") === col("LocationID")
        || col("PULocationID") === col("LocationID"), "left")
      .groupBy(col("Borough"))
      .count()
      .orderBy(col("count").desc)
  }

  def saveData(df: DataFrame, path: String): Unit = {
    df
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(path)
  }

  val taxiFactsDF = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
  val taxiZonesDF = readCSV("src/main/resources/data/taxi_zones.csv")

  val TopBoroughDF = getTopBorough(taxiFactsDF, taxiZonesDF)

  saveData(TopBoroughDF, "src/main/resources/data/out/top_borough")

  TopBoroughDF.show()
}
