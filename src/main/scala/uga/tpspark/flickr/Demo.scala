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

object Demo {
  def main(args: Array[String]): Unit = {
    println("hello")
    var spark: SparkSession = null
    try {
      spark = SparkSession.builder().appName("Flickr using dataframes").getOrCreate()
      val textFile: RDD[String] = spark.sparkContext.textFile("textFile.txt")
      val lineLength: RDD[Int] = textFile.map(line => line.length())
      val lineWithNumber: RDD[(String, Long)] = textFile.zipWithIndex()
      val lineNumberWithLength: RDD[(Long, Int)] = lineWithNumber.map(f => (f._2, f._1.length()))
      val allWords: RDD[String] = textFile.flatMap(line => line.split("\\s+"))
    } catch {
      case e: Exception => throw e
    } finally {
      spark.stop()
    }
    println("done")
  }
}