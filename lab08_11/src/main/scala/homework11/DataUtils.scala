package homework11

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataUtils {

  def readParquet(path: String)(implicit spark: SparkSession): DataFrame =
    spark
      .read
      .parquet(path)

  def readCSV(path: String)(implicit spark: SparkSession):DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

}
