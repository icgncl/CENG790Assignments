
package edu.metu.ceng790.hw2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession



object Part1 {
  def main(args: Array[String]): Unit = {
    // In order to show only errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Recommendation System").config("spark.master", "local[*]").getOrCreate()
    // Reading the file
    val data_with_header= spark.sparkContext.textFile("ml-20m/ratings.csv")
    // In order to remove header from RDD
    val data_header = data_with_header.first()
    val data = data_with_header.filter(x => x!=data_header)
    // Visualizing the first 10 rows
    data.take(10).foreach(println)
    // In order to use "Rating", I have used https://spark.apache.org/docs/latest/mllib-collaborative-filtering.html website
    val ratings = data.map(_.split(',') match { case Array(user, item, rate, ts) =>
      Rating(user.toInt, item.toInt, rating = rate.toDouble)})
    // ---------------------------------------------- //
    // In order to calculate mean of rating for each user, Rating is grouped according to user
    val ratings_grouped_by_user = ratings.groupBy(line => line.user)
    // Sum of ratings per user is mapped as -> (user, Sum of ratings)
    val sum_of_ratings_per_user = ratings_grouped_by_user.map(x => (x._1, x._2.map(coproduct => coproduct.rating).sum))
    // Number of ratings per used is mapped as -> (user, Number of ratings)
    val number_of_ratings_per_used = ratings_grouped_by_user.map(x => (x._1, x._2.size))
    // Avg Rating Per User and convert it to Map (https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.collectAsMap.html)
    val avg_movie_rating = sum_of_ratings_per_user.join(number_of_ratings_per_used).
                  map{case (user, (sum_of_ratings, number_of_ratings))
                      => (user, sum_of_ratings/number_of_ratings)}.collectAsMap()

    // In order to normalize ratings, we need to subtract the average ratings for each rating
    // I subtracted avg rating of user from ratings
    val ratings_w_normalize = ratings.map( f =>
      Rating(f.user,
        f.product,
        (f.rating - (avg_movie_rating(f.user)
          )
      )))
    // ---------------------------------------------- //
    // Split Data to Train and Test Set
    val (train_set, test_set) = ALSParameterTuning.data_splitter(ratings_w_normalize)
    // ---------------------------------------------- //
    // MODEL AND TRAINING //
    // Declaration of parameters
    val rank_variables=Array(8, 12)
    val iterations_variables=Array(10, 20)
    val lambda_variables = Array(0.01, 1.0, 10.0)
    // Declaration of the model
    for (each_rank <- rank_variables){
      for (each_iteration <- iterations_variables){
        for (each_lambda <- lambda_variables){
          // Training
          val model = ALS.train(train_set, rank = each_rank, iterations = each_iteration, lambda = each_lambda)
          // Predicting
          val predictions = model.predict(test_set.map(line => (line.user, line.product))).map(x =>(x.user, x.product, x.rating + avg_movie_rating(x.user)))
        }
      }
    }

    spark.stop()
  }
}
