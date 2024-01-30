package com.bdec.training.sparkscala

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}


object LogisticRegression {
  val diabetesURL = "file:///C:\\Training\\ml_dataset\\diabetes.csv"

  def main(args: Array[String]): Unit = {
    val winutilPath = "C:\\softwares\\winutils"

    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      System.out.println("Detected windows")
      System.setProperty("hadoop.home.dir", winutilPath)
      System.setProperty("HADOOP_HOME", winutilPath)
    }

    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    val rawDf = spark.read.option("header", "true").option("inferSchema", "true").csv(diabetesURL)
    val requiredFields = rawDf.na.drop

    val fieldsForTraining = requiredFields.selectExpr("cast(Pregnancies as double)", "cast(Glucose as double)", "cast(BloodPressure as double)", "cast(SkinThickness as double)", "cast(Insulin as double)", "cast(BMI as double)", "cast(DiabetesPedigreeFunction as double)", "cast(Age as double)", "cast(Outcome as double)").withColumnRenamed("Outcome", "label")
    fieldsForTraining.show()
    fieldsForTraining.printSchema()
    val inputCols: Array[String] = Array[String]("Pregnancies", "Glucose", "BloodPressure", "SkinThickness", "Insulin", "BMI", "DiabetesPedigreeFunction", "Age")

    val assembler = new VectorAssembler().setInputCols(inputCols).setOutputCol("features")
    val finalDf = assembler.transform(fieldsForTraining)

    finalDf.show()
    finalDf.printSchema()
    val trainTestDf: Array[DataFrame] = finalDf.randomSplit(Array[Double](0.8, 0.2))
    val tainDf = trainTestDf(0)
    val testDf = trainTestDf(1)

    val lr: LogisticRegression = new LogisticRegression
    val trainedModel: LogisticRegressionModel = lr.fit(tainDf)
    println(trainedModel.coefficients)
    val testPredictionsDf = trainedModel.transform(testDf)
    testPredictionsDf.show()
    val evaluator: Double = new BinaryClassificationEvaluator().evaluate(testPredictionsDf)
    println("accuracy = " + evaluator)

  }
}
