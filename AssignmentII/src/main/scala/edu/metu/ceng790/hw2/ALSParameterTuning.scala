package edu.metu.ceng790.hw2

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD


object ALSParameterTuning {
  def Data_splitter(ratings_w_normalize: RDD[Rating]): (RDD[Rating], RDD[Rating]) = {

    // Divide data to training and test sets
    val Array(train_set, test_set) = ratings_w_normalize.randomSplit(Array[Double](0.9, 0.1), seed = 18)
    (train_set, test_set)
  }

  def Msecalculator(predictions: RDD[((Int, Int), (Double, Double))]): Double = {
    predictions.map { case ((user, product), (rating, predicted)) =>
      (rating - predicted) * (rating - predicted)
    }.mean()
  }
}
