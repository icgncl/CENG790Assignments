
package edu.metu.ceng790.hw2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession

import scala.util.Random


object nearestneighbors {
  def parseLineforMovieswithGenres(line: String): (Int, Array[String]) = {
    val fields = line.split(",")
    val movieID = fields(0).toInt
    val genres = fields(2).split("\\|")
    return (movieID, genres)
  }
  def main(args: Array[String]): Unit = {
    // In order to show only errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Recommendation System").config("spark.master", "local[*]").getOrCreate()

    // --------------------------------------- //
    // --------------- PART II --------------- //
    // --- Content-based Nearest Neighbors --- //
    println("PART II - Content-based Nearest Neighbors")

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

    //-------------------------- PART 2.1 --------------------------//
    // In order to only keep ratings which are higher than avg. ratings
    val ratings_w_normalize = ratings.filter( f =>
      f.rating > avg_movie_rating(f.user)
      )

    //-------------------------- PART 2.2 --------------------------//
    // Read movies csv file
    val movies_file_w_header = spark.sparkContext.textFile("ml-20m/movies.csv")
    // This part is also made in Part 1 therefore I copied this part
    val data_header_for_movies = movies_file_w_header.first()
    val movies_data = movies_file_w_header.filter(x => x != data_header_for_movies)
    val movieNames = movies_data.map(collaborative_filtering.parseLineforMovies).collectAsMap()

    //-------------------------- PART 2.3 --------------------------//
    val movieGenres = movies_data.map(nearestneighbors.parseLineforMovieswithGenres).collectAsMap()
    spark.stop()
  }
}
