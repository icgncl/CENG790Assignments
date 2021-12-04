package edu.metu.ceng790.hw1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class Picture(fields: Array[String]) {
  val lat: Double = fields(11).toDouble
  val lon: Double = fields(10).toDouble
  val c: Country = Country.getCountryAt(lat, lon)
  val userTags: Array[String] = java.net.URLDecoder.decode(fields(8), "UTF-8").split(",")
  def hasTags: Boolean = {
    userTags.size > 0 && !(userTags.size == 1 && userTags(0).isEmpty())
  }
}

object Part2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    var spark: SparkSession = null
    spark = SparkSession.builder().appName("Flickr using dataframes").config("spark.master", "local[*]").getOrCreate()
    val originalFlickrMeta: RDD[String] = spark.sparkContext.textFile("flickrSample.txt")

    // YOUR CODE HERE

    // Display the 10 lines of the RDD
    originalFlickrMeta.take(10).foreach(println)

    // Number of lines in RDD
    println(f"Number of lines in the RDD : ${originalFlickrMeta.count()}")

    // Mapping - firstly line is splitted to Array with regex (" ")
    val array_RDD = originalFlickrMeta.map( line => line.split("\t"))

    // Creating Picture map
    val picture_RDD = array_RDD.map(Picture)
    // Filtering
    val filtered_pictures = picture_RDD.filter(x => x.c != null).filter(x => x.hasTags == true)

    // Displaying the top 10 country codes and user tags
    filtered_pictures.take(10).foreach(picture => println(f"Country is ${picture.c}, User tags: ${picture.userTags.mkString(" ")}"))

    println("PART 2.3")
    // Group by country and taking the first country and print countrycode and user tags
    val pictures_g_country = filtered_pictures.map(x => (x.c,x.userTags.mkString(","))).groupByKey
    val countrycode = pictures_g_country.take(1).map(_._1).mkString(" ")
    val userTags = pictures_g_country.take(1).map(_._2).map(x => x.map(x => x.mkString))
    println(f"Countrycode is ${countrycode}")
    println("UserTags:")
    userTags.foreach(x => x.foreach(x => println(f"|-> ${x}")))

    println("PART 2.4")
    // Firstly grouped by key and then userTags are flatted, finally they are printed
    val country_w_Array_userTags = filtered_pictures.map(x => (x.c, x.userTags)).groupByKey()
    val country_w_userTags = country_w_Array_userTags.map(x => (x._1,x._2.flatten))
    country_w_userTags.foreach(println)

    println("PART 2.5")
    // In this part, I have used groupby(identity) as indicated in the question, also
    // I have used https://www.java-success.com/10-%E2%99%A5-coding-scala-way-groupby-mapvalues-identity/ -> for .size
    country_w_userTags.map(f=> (f._1, f._2.groupBy(identity).map(y => (y._1, y._2.size)))).foreach(println)

    // This is critical in order to stop cluster
    spark.stop()
  }
}
