package Ben.com.ETL

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import Ben.com.Util._

class movielens1M {
  def main(args: Array[String]) {

    val read_coalesce = 200
    val read_movie_coalesce = 20

    val conf = new SparkConf().setAppName("dataView")
    val sc = new SparkContext()
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val data = sc.textFile(StringUtils.DATA_PATH, read_coalesce).map(line => {
      val li = line.split("::")
      caseUtils.movieLens(li(0).toInt, li(1).toInt, li(2).toDouble, li(3).toLong % 10)
    }).toDF()

    val movie_data = sc.textFile(StringUtils.MOVIE_DATA_PATH, read_movie_coalesce).map(line => {
      val li = line.split("::")
      caseUtils.movie(li(0).toInt, li(1), li(2))
    }).toDF()

    /* user fequency */
    data.groupBy("user").count().show()

    /* movie fequency */
    data.join(movie_data, data("movie") === movie_data("id")).groupBy(movie_data("title")).count().show()

    /* count, mean, stddev, min, and max of rating */
    data.describe("rating", "timstamp").show()
  }
}