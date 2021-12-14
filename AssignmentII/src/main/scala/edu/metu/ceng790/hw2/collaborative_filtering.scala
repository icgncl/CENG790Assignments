package edu.metu.ceng790.hw2



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
  def elicitateRatings(selectedMovies: List[(Int, String)]): (Int) = {

    return 1
  }
}
