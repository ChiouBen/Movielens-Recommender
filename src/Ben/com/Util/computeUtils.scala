package Ben.com.Util

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.Rating
import java.util.Calendar
import java.text.SimpleDateFormat

object computeUtils extends Serializable {

  def precision() {}

  def recall() {}

  def computeRMSE(model: MatrixFactorizationModel, testdata: RDD[Rating]): Double = {

    val usersProducts = testdata.map {
      case Rating(user, product, rate) =>
        (user, product)
    }
    val predictions =
      model.predict(usersProducts).map {
        case Rating(user, product, rate) =>
          ((user, product), rate)
      }
    val ratesAndPreds = testdata.map {
      case Rating(user, product, rate) =>
        ((user, product), rate)
    }.join(predictions)

    val MSE = ratesAndPreds.map {
      case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
    }.mean()

    Math.sqrt(MSE)
  }

  def getRecList(model: MatrixFactorizationModel, testdata: RDD[Rating], N: Int) = {
    val usersProducts = testdata.map {
      case Rating(user, product, rate) =>
        (user, product)
    }

    model.predict(usersProducts).map {
      case Rating(user, product, rate) =>
        (user, (product, rate))
    }.groupByKey().map(line => (line._1, topN(line._2, N)))
  }

  def topN(user_rating: scala.collection.Iterable[(Int, Double)], number: Int): List[Int] = {
    user_rating.toList.sortWith((x, y) => {
      val xx = if (x._2 > 0) x._2 else 0
      val yy = if (y._2 > 0) y._2 else 0
      xx > yy
    }).take(number).map(line => line._1)
  }

}