
package edu.metu.ceng790.hw2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random
import scala.util.control._


object nearestneighbors {

  def userSim(user1: RDD[(Int, Map[String, Int])], user2: RDD[(Int, Map[String, Int])]): (Double) = {
    val user1_genres = user1.map(x => x._2).collect()(0)
    val user2_genres = user2.map(x => x._2).collect()(0)
    var length_of_user1 = 0.0
    var length_of_user2 = 0.0
    var similarity_mult = 0.0

    // Calculating the length of the vectors
    for (each_genre <- user1_genres) {
      length_of_user1 += (each_genre._2) * (each_genre._2)
    }
    for (each_genre <- user2_genres) {
      length_of_user2 += (each_genre._2) * (each_genre._2)
    }
    length_of_user1 = Math.sqrt(length_of_user1)
    length_of_user2 = Math.sqrt(length_of_user2)
    // --------------//

    for (each_genre <- user1_genres) {
      Breaks.breakable {
        try {
          val genre_value = user2_genres(each_genre._1)
          val mult_of_genre_values = genre_value * user1_genres(each_genre._1)
          similarity_mult += mult_of_genre_values
        }
        catch {
          case _ =>
            Breaks.break
        }
      }
    }
    val similarity = (similarity_mult / length_of_user1) / length_of_user2
    return similarity
  }

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
    val data_with_header = spark.sparkContext.textFile("ml-20m/ratings.csv")
    // In order to remove header from RDD
    val data_header = data_with_header.first()
    val data = data_with_header.filter(x => x != data_header)
    // Visualizing the first 10 rows
    data.take(10).foreach(println)
    // In order to use "Rating", I have used https://spark.apache.org/docs/latest/mllib-collaborative-filtering.html website
    val ratings = data.map(_.split(',') match { case Array(user, item, rate, ts) =>
      Rating(user.toInt, item.toInt, rating = rate.toDouble)
    })
    // ---------------------------------------------- //
    // In order to calculate mean of rating for each user, Rating is grouped according to user
    val ratings_grouped_by_user = ratings.groupBy(line => line.user)
    // Sum of ratings per user is mapped as -> (user, Sum of ratings)
    val sum_of_ratings_per_user = ratings_grouped_by_user.map(x => (x._1, x._2.map(coproduct => coproduct.rating).sum))
    // Number of ratings per used is mapped as -> (user, Number of ratings)
    val number_of_ratings_per_used = ratings_grouped_by_user.map(x => (x._1, x._2.size))
    // Avg Rating Per User and convert it to Map (https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.collectAsMap.html)
    val avg_movie_rating = sum_of_ratings_per_user.join(number_of_ratings_per_used).
      map { case (user, (sum_of_ratings, number_of_ratings))
      => (user, sum_of_ratings / number_of_ratings)
      }.collectAsMap()

    //-------------------------- PART 2.1 --------------------------//
    // In order to only keep ratings which are higher than avg. ratings
    val ratings_w_normalize = ratings.filter(f =>
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
    //--------------------------------------------------------------//

    //-------------------------- PART 2.4 --------------------------//
    val user_w_genres = ratings_w_normalize.map(f => (f.user, movieGenres(f.product))).groupByKey.map(eachuser => (eachuser._1, eachuser._2.toArray.flatten))
    val userVectors = user_w_genres.map(each_user => (each_user._1, each_user._2.groupBy(identity).map(each_genre => (each_genre._1, each_genre._2.size))))

    //--------------------------------------------------------------//

    //-------------------------- PART 2.5 --------------------------//
    val userSimilarity = nearestneighbors.userSim(userVectors.filter(x => x._1 == 1), userVectors.filter(x => x._1 == 4179))
    spark.stop()
  }
}
