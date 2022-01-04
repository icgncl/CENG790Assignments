package edu.metu.ceng790.hw3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Credit {
  // define the Credit Schema
  case class Credit(
                     creditability: Double,
                     balance: Double, duration: Double, history: Double, purpose: Double, amount: Double,
                     savings: Double, employment: Double, instPercent: Double, sexMarried: Double, guarantors: Double,
                     residenceDuration: Double, assets: Double, age: Double, concCredit: Double, apartment: Double,
                     credits: Double, occupation: Double, dependents: Double, hasPhone: Double, foreign: Double
                   )

  // function to create a  Credit class from an Array of Double
  def parseCredit(line: Array[Double]): Credit = {
    Credit(
      line(0),
      line(1) - 1, line(2), line(3), line(4), line(5),
      line(6) - 1, line(7) - 1, line(8), line(9) - 1, line(10) - 1,
      line(11) - 1, line(12) - 1, line(13), line(14) - 1, line(15) - 1,
      line(16) - 1, line(17) - 1, line(18) - 1, line(19) - 1, line(20) - 1
    )
  }

  // function to transform an RDD of Strings into an RDD of Double
  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    rdd.map(_.split(",")).map(_.map(_.toDouble))
  }

  def main(args: Array[String]) {

    // In order to show only errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("AssignmentIII")
      .getOrCreate();

    val sc = spark.sparkContext

    // load the data into a RDD
    val creditRDD = parseRDD(sc.textFile("credit/credit.csv")).map(parseCredit)

    // Converting RDD to DF
    val creditDF = spark.createDataFrame(creditRDD)

    // PART 1 //
    // Vector Assembler
    val vector_assembler = new VectorAssembler().
      setInputCols(
        Array("balance", "duration", "history", "purpose", "amount", "savings", "employment",
          "instPercent", "sexMarried", "guarantors", "residenceDuration", "assets", "age", "concCredit",
          "apartment", "credits", "occupation", "dependents", "hasPhone", "foreign")).setOutputCol("all_features")


    // PART 2 //
    val creditability_indexer = new StringIndexer()
      .setInputCol("creditability")
      .setOutputCol("creditabilityIndex")

    // PART 3 //
    // Train test split with ratio %90 to %10 and seed was set
    // in order to get same test and train set in each run
    val Array(train_set, test_set) = creditDF.randomSplit(Array[Double](0.9, 0.1), seed = 18)

    // PART 4 //
    // train Random Forest model with training data set
    val rfc = new RandomForestClassifier()
      .setSeed(1234)
      .setFeaturesCol("all_features")
      .setLabelCol("creditabilityIndex")
      .setFeatureSubsetStrategy("auto")

    // grid search parameters are given
    val grid_search = new ParamGridBuilder()
      .addGrid(rfc.maxDepth, Array(4, 6, 8))
      .addGrid(rfc.maxBins, Array(25, 28, 31))
      .addGrid(rfc.impurity, Array("entropy", "gini"))
      .build()

    // PART 5 //
    // Pipeline object is created
    val pipeline = new Pipeline()
      .setStages(Array(vector_assembler, creditability_indexer, rfc))


    // Creating train validation split
      // BinaryClassificationEvaluator is used
      // 0.75 ratio means 3x -> train set x -> validation set
    val train_val_split = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEstimatorParamMaps(grid_search)
      .setEvaluator(new BinaryClassificationEvaluator().setLabelCol("creditabilityIndex"))
      .setTrainRatio(0.75)

    // Training the model
    val model_fit = train_val_split.fit(train_set)

    // Getting the best parameters
    // I could not find the best parameters because I have used Pipeline
    println(model_fit.bestModel.extractParamMap())

    // PART 6 //
    // Make predictions.
    val predictions_test = model_fit.transform(test_set)
    val predictions_train = model_fit.transform(train_set)

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("creditabilityIndex")

    val accuracy_test = evaluator.evaluate(predictions_test)
    val accuracy_train = evaluator.evaluate(predictions_train)
    println(s"Train Accuracy = ${accuracy_train}")
    println(s"Test Accuracy = ${accuracy_test}")





    spark.stop()


  }
}
