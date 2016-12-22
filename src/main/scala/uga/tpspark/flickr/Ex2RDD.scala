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
import org.apache.spark.rdd.RDD

object Ex2RDD {
  def main(args: Array[String]): Unit = {
    var spark: SparkSession = null
    try {
      spark = SparkSession.builder().appName("Flickr using dataframes").getOrCreate()
      
      // 1 - Read from input file
      val originalFlickrMeta: RDD[String] = spark.sparkContext.textFile("flickrSample.txt")
      originalFlickrMeta.take(5).foreach(println)
      println(originalFlickrMeta.count())
      
      // 2 - Convert RDD[String] to RDD[Picture]
      val pictures: RDD[Picture] = originalFlickrMeta.map(line => new Picture(line.split("\t")))
      pictures.take(5).foreach(println)
      println(pictures.count())
      
      // 2 - Filter pictures to get Interesting Pictures
      val interestingPictures: RDD[Picture] = pictures.filter(picture => ((picture.hasValidCountry) && (picture.hasTags)))
      interestingPictures.take(5).foreach(println)
      println(interestingPictures.count())

      // 3 - Group Pictures by Country 
      val picturesByCountry: RDD[(Country, Iterable[Picture])] = interestingPictures.groupBy(p => p.c)
      println("Type of RDD : " + picturesByCountry.getClass)
      picturesByCountry.take(1).foreach(println)
      
      //4 - Get Tags grouped by Country
      val tagsByCountry: RDD[(Country, Iterable[String])] = picturesByCountry.map(cp => (cp._1, cp._2.flatten(p => p.userTags)))
      tagsByCountry.take(5).foreach(println)
      println(tagsByCountry.count())
      
      // 3,4 - Could also be done like this
      //      val picturesByCountry = interestingPictures.map(picture => (picture.c, picture.userTags)).groupByKey()
      //
      //      picturesByCountry.take(5).foreach(line => println(line._1 + ":[" + line._2.map(_.mkString(",")).mkString("],[") + "]"))
      //      println(picturesByCountry.count())
      //      println("Type of RDD : " + picturesByCountry.getClass)
      //      
      //      val tagsByCountry = picturesByCountry.map(line => (line._1, line._2.flatten))
      
      // 5 - Get Map of tags with their frequencies grouped by country.
      val tagCountMapByCountry = tagsByCountry.map(line => (line._1, line._2.groupBy(identity).mapValues(_.size).map(identity)))
      println(tagCountMapByCountry.getClass)
      tagCountMapByCountry.take(5).foreach(println)
      
      // 6 - Another way to get the above final result
      // Create tuple of (country, tag) as key and 1 as value to help count the sum of tags per country
      val countryTagKey = interestingPictures.flatMap {
        case(picture) => {
          picture.userTags.map(tag => ((picture.c, tag), 1))
        }
      }
      countryTagKey.take(5).foreach(println)
      println(countryTagKey.count())
      
      // Reduce the above rdd by key and get the sum (count) of values
      val CountByCountryTag = countryTagKey.reduceByKey(_ + _)
      CountByCountryTag.take(5).foreach(println)
      
      // To get a map of tags with counts grouped by country, create RDD with key as country and value as tuple of (tag, count)
      val TagCountByCountry = CountByCountryTag.map {
        case((country, tag), count) => {
          (country, Map(tag -> count))
        }
      }
      TagCountByCountry.take(5).foreach(println)
      
      // Reduce the above RDD by key (Country) to get map of tags with their frequencies grouped by key
      // ++ concatenates two maps
      val TagCountMapByCountry = TagCountByCountry.reduceByKey(_ ++ _)
      TagCountMapByCountry.take(5).foreach(println)

    } catch {
      case e: Exception => throw e
    } finally {
      spark.stop()
    }
    println("done")
  }
}