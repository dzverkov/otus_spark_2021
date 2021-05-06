package homework11

import homework11.DataUtils.{readCSV, readParquet}
import homework11.HwTask01.getTopBorough
import homework11.HwTask03.{getTripStat, loadData}
import homework11.RddTypes.{TripDist, TripStat}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.catalyst.encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.test.SQLTestUtils

//import scala.tools.fusesource_embedded.jansi.AnsiRenderer.test

class DFDSTask0103Test extends AnyFunSuite {

  implicit val spark = SparkSession.builder()
    .appName("TestTask11_02")
    .config("spark.master", "local")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  test("Test - Task 01") {

    val taxiZonesDF: DataFrame = readCSV("src/main/resources/data/taxi_zones.csv")
    val taxiFactsDF: DataFrame = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val resDF: DataFrame = getTopBorough(taxiFactsDF, taxiZonesDF)

    val expectedResDF: Seq[Row] =
      Row("Manhattan", 583291) ::
      Row("Queens", 30547) ::
      Row("Brooklyn", 15336) ::
      Row("Unknown", 7692) ::
      Row("Bronx", 1708) ::
      Row("EWR", 510) ::
      Row("Staten Island", 64) :: Nil

    checkAnswer(resDF, expectedResDF)
  }

  test("Test - Task 03") {

    val distDS: Dataset[TripDist] = loadData("src/main/resources/data/yellow_taxi_jan_25_2018")
    val resDF: DataFrame = getTripStat(distDS).toDF

    val expectedResDF: Seq[Row] = Row(330023, 66.0, 0.01, 3.0, 3.49) :: Nil

    checkAnswer(resDF, expectedResDF)
  }

}

