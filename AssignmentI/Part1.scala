package edu.metu.ceng790.hw1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Part1 {
  def main(args: Array[String]): Unit = {
    // In order to show only errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Flickr using dataframes").config("spark.master", "local[*]").getOrCreate()

    //   * Photo/video identifier
    //   * User NSID
    //   * User nickname
    //   * Date taken
    //   * Date uploaded
    //   * Capture device
    //   * Title
    //   * Description
    //   * User tags (comma-separated)
    //   * Machine tags (comma-separated)
    //   * Longitude
    //   * Latitude
    //   * Accuracy
    //   * Photo/video page URL
    //   * Photo/video download URL
    //   * License name
    //   * License URL
    //   * Photo/video server identifier
    //   * Photo/video farm identifier
    //   * Photo/video secret
    //   * Photo/video secret original
    //   * Photo/video extension original
    //   * Photos/video marker (0 = photo, 1 = video)

    val customSchemaFlickrMeta = StructType(Array(
      StructField("photo_id", LongType, true),
      StructField("user_id", StringType, true),
      StructField("user_nickname", StringType, true),
      StructField("date_taken", StringType, true),
      StructField("date_uploaded", StringType, true),
      StructField("device", StringType, true),
      StructField("title", StringType, true),
      StructField("description", StringType, true),
      StructField("user_tags", StringType, true),
      StructField("machine_tags", StringType, true),
      StructField("longitude", FloatType, false),
      StructField("latitude", FloatType, false),
      StructField("accuracy", StringType, true),
      StructField("url", StringType, true),
      StructField("download_url", StringType, true),
      StructField("license", StringType, true),
      StructField("license_url", StringType, true),
      StructField("server_id", StringType, true),
      StructField("farm_id", StringType, true),
      StructField("secret", StringType, true),
      StructField("secret_original", StringType, true),
      StructField("extension_original", StringType, true),
      StructField("marker", ByteType, true)))

    val originalFlickrMeta = spark.sqlContext.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .schema(customSchemaFlickrMeta)
      .load("flickrSample.txt")

    // YOUR CODE HERE
    println(originalFlickrMeta.count())

    // Following line prints the schema so that I can see names of fields
    originalFlickrMeta.printSchema()

    // Following line craetes a view in order to use it with SQL
    originalFlickrMeta.createOrReplaceTempView("flickrdata")

    // In order to get photo_id, GPS coordinates and license type SQL is used
    val all_data =spark.sql("SELECT distinct photo_id, longitude, latitude, license from flickrdata")
    all_data.show()
    // Filtered data and it is cached for Part I.5b
    // FOR Part I.5b -> at the end of line -> .cache() must be added so that DataFrame will be cached
    //                 in the memory if there is enough space.
    val filtered_data = all_data.filter("latitude != -1 and latitude != -1").filter("license not like '%Attribution-ShareAlike%'") //.cache()

    // Display the execution plan
    filtered_data.explain()
    // In order to display the data
    filtered_data.show()

    // In order to read license file
    val license_file = spark.sqlContext.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .load("flickrLicense.txt")

    // Following line craetes a view in order to use it with SQL
    license_file.createOrReplaceTempView("license_data")
    filtered_data.createOrReplaceTempView("filtered_data")

    // Combining the data and select Nonderivates
    val filtered_DF = spark.sql("SELECT * from filtered_data")
    val license_DF = spark.sql("SELECT * from license_data")
    val combined_DF = filtered_DF.join(license_DF, filtered_data("license") === license_DF("Name"))
    val final_DF = combined_DF.filter("NonDerivative == 1")

    //Explain the combined data
    final_DF.explain()

    //Showing the combined data
    final_DF.show()

    // Saving to a single CSV file with header
    final_DF.coalesce(numPartitions = 1).write.option("header", true).csv("final_output.csv")

    spark.stop()
  }
}
