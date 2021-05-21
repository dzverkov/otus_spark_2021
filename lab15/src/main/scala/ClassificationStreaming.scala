import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, from_csv, udf}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object ClassificationStreaming extends App{

  val spark = SparkSession.builder()
    .appName("ClassificationStreaming")
    .config("spark.master", "local[*]")
    .config("spark.driver.host","localhost")
    .getOrCreate()

  import spark.implicits._

  val labels = Map(0.0 -> "setosa", 1.0 -> "versicolor", 2.0 -> "virginica")

  val model = PipelineModel.load("src/main/resources/model")

  val irisSchema = StructType(
    StructField("sepal_length", DoubleType, nullable = true) ::
      StructField("sepal_width", DoubleType, nullable = true) ::
      StructField("petal_length", DoubleType, nullable = true) ::
      StructField("petal_width", DoubleType, nullable = true) ::
      Nil
  )

  val irisDataDF: DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "earliest")
    .option("subscribe", "input")
    .load()
    .withColumn("struct", from_csv($"value".cast(StringType), irisSchema, Map("sep" -> ",")))
    .withColumn("sepal_length", $"struct".getField("sepal_length"))
    .withColumn("sepal_width", $"struct".getField("sepal_width"))
    .withColumn("petal_length", $"struct".getField("petal_length"))
    .withColumn("petal_width", $"struct".getField("petal_width"))
    .drop("value", "struct")

  val assembler = new VectorAssembler()
    .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
    .setOutputCol("features")

  val irisData = assembler.transform(irisDataDF)

  val getPredictionLabel = (col: Double) => {
    labels(col)
  }
  val predictionLabel = udf(getPredictionLabel)

  val prediction = model.transform(irisData)

  val query = prediction
    .withColumn("predictionLabel", predictionLabel(col("prediction")))
    .select(
      concat_ws(",",$"sepal_length", $"sepal_width", $"petal_length", $"petal_width", $"predictionLabel").as("value")
    )
    .writeStream
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("checkpointLocation", "/tmp/checkpoint")
    .option("topic", "prediction")
    .start()

  query.awaitTermination()

}
