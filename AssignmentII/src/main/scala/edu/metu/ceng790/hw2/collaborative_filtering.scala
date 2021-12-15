package edu.metu.ceng790.hw2

import org.apache.spark
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn.readLine


object collaborative_filtering {
  def parseLineforMovies(line: String): (Int, String) = {
    val fields = line.split(",")
    val movieID = fields(0).toInt
    val title = fields(1)
    return (movieID, title)
  }

  def parseLineforRatings(line: String): (Int) = {
    val fields = line.split(",")
    val movieID = fields(1).toInt
    //val rating = fields(2).toDouble
    return (movieID)
  }

  def elicitateRatings(selectedMovies: List[(Int, String)]): (Array[Rating]) = {
    val user_id = 19031903
    var format_is_correct = 0
    var terminal_user_ratings =  Array.empty[Rating]
    for (movie <- selectedMovies) {
      format_is_correct = 0
      while (format_is_correct < 1) {
        println("Give a rating for movie:" + movie._2)
        val user_rating = readLine()
        try {
          // "If" executed whether format of rating is correct
          if (user_rating.toInt >= 0 && user_rating.toInt <= 5) {
            val movie_rating = Rating(user_id, movie._1, user_rating.toInt)
            terminal_user_ratings = terminal_user_ratings ++ Array(movie_rating)
            format_is_correct = 1
          }
          else {
            println("Please give a rating between 1 and 5 (if you don't know the movie you can give 0)")
          }
        } catch {
          case _: Exception => println("Please give a rating between 1 and 5 (if you don't know the movie you can give 0)")
        }
      }
    }


  return terminal_user_ratings

}
}
