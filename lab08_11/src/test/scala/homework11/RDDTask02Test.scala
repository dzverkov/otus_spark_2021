package homework11

import homework11.HwTask02.{getTopTime, loadData}
import homework11.RddTypes.TaxiRide
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class RDDTask02Test extends AnyFlatSpec {

  implicit val spark = SparkSession.builder()
    .appName("TestTask11_02")
    .config("spark.master", "local")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  it should "successfully calculate the distribution by hour" in {

    val taxiFactsRDD: RDD[TaxiRide] = loadData("src/main/resources/data/yellow_taxi_jan_25_2018")
    val TopTimeRDD = getTopTime(taxiFactsRDD)

    val expectedResults: Seq[(Int, Int)] = Seq(
      (19, 22121), (20, 21598), (22, 20884), (21, 20318), (23, 19528), (9, 18867), (18, 18664), (16, 17843),
      (15, 17483), (10, 16840), (17, 16160), (14, 16082), (13, 16001), (12, 15564), (8, 15445), (11, 15348),
      (0, 14652), (7, 8600), (1, 7050), (2, 3978), (6, 3133), (3, 2538), (4, 1610), (5, 1586)
    )
    val expectedResultsRDD: RDD[(Int, Int)] = spark.sparkContext.parallelize(expectedResults)

    assert(TopTimeRDD.collect sameElements expectedResultsRDD.collect)
  }
}