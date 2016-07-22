package Ben.com.Test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object run {
  val conf = new SparkConf().setAppName("test")
  val sc = new SparkContext()
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  import sqlContext.implicits._
  case class Rating(userid: String, movieid: String, rating: Int, timestamp: Int)
  case class User(userid: String, gender: String, age: Int, zipcode: String)
  case class Movie(movieid: String, title: String, genres: List[String])
  val people = sc.textFile("/recommender/movielens_data/1m_data/ratings.dat").map(_.split("::")).map(p => Rating(p(0), p(1),p(2).trim.toInt,p(3).trim.toInt)).toDF()
  val userStatistic=people.select("userid","movieid")
}