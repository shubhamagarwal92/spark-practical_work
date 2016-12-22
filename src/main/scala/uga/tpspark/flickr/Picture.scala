package uga.tpspark.flickr

class Picture(fields: Array[String]) extends Serializable {
  val lat: Double = fields(11).toDouble
  val lon: Double = fields(10).toDouble
  val c: Country = Country.getCountryAt(lat, lon)
  val userTags: Array[String] = java.net.URLDecoder.decode(fields(8), "UTF-8").split(",")

  def hasValidCountry: Boolean = {
    c != null
  }

  def hasTags: Boolean = {
    userTags.size > 0 && !(userTags.size == 1 && userTags(0).isEmpty())
  }

  override def toString: String = "(" + c + ", " + userTags.reduce((t1: String, t2: String) => t1 + ", " + t2) + ")"

}