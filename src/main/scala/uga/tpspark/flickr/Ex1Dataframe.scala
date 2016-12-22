package uga.tpspark.flickr

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import java.net.URLDecoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoders

object Ex1Dataframe {
  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
    try {
      spark = SparkSession.builder().appName("Flickr using dataframes").getOrCreate()

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
        
      originalFlickrMeta.registerTempTable("originalFlickrMeta")
      
      // 1 - Select identifier, GPS coordinates, and type of license for each picture
      val customFlickrMeta = spark.sql("SELECT photo_id, latitude, longitude, license FROM originalFlickrMeta") 
      customFlickrMeta.registerTempTable("customFlickrMeta")      
      customFlickrMeta.show()
      
      // 2 - Create DataFrame of interesting pictures by filtering
      val interestingPictures = spark.sql("SELECT * FROM customFlickrMeta WHERE license IS NOT NULL AND longitude != -1 AND latitude != -1")
      interestingPictures.registerTempTable("interestingPictures")  
      
      // 3 - Display execution plan used by Spark
      interestingPictures.explain()
      
      // 4 - Display data of the interesting pictures
      interestingPictures.show()

      // 6 - Cache the interesting pictures DataFrame to avoid recomputing it from file each time it is called
      interestingPictures.cache()

      // 5 - Read license data and join it with interesting pictures to get pictures which are also non derivative
      val licenseFlickrMeta = spark.sqlContext.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "True")
        .load("FlickrLicense.txt")
      
      licenseFlickrMeta.registerTempTable("licenseFlickrMeta")  
      licenseFlickrMeta.show()
      
      val interestingNonDerivativePictures = spark.sql("""SELECT i.* from interestingPictures i 
                                join licenseFlickrMeta l on i.license == l.Name
                                where l.NonDerivative=1 
                                """)
      interestingNonDerivativePictures.show()                                
      interestingNonDerivativePictures.explain()

      // 7 - Save the final DataFrame in a CSV file with headers
      interestingNonDerivativePictures.write
        .format("csv")
        .option("header", "true")
        .save("interestingNonDerivativePictures.csv")

      
    } catch {
      case e: Exception => throw e
    } finally {
      spark.stop()
    }
    println("done")
  }
}