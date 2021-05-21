import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

object TrainModel extends App {

  val spark = SparkSession.builder()
    .appName("ModelTrain")
    .config("spark.master", "local[*]")
    .config("spark.driver.host","localhost")
    .getOrCreate()

  val data = spark.read.format("libsvm").load("src/main/resources/data/iris_libsvm.txt")

  val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

  val rf = new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setNumTrees(10)

  val pipeline = new Pipeline()
    .setStages(Array(rf))

  val model = pipeline.fit(trainingData)

  val predictions = model.transform(testData)

  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

  val accuracy = evaluator.evaluate(predictions)
  println(s"Test Error = ${1.0 - accuracy}")

  model.write.overwrite().save("src/main/resources/model")

}
