package Ben.com.Util

import scala.collection.mutable.Map
import scala.math._
import scala.util.Random
import scala.util.Random

/**
 * @author BenChiou
 */
@transient
object userBasedUtil extends Serializable {

  def calculateMeans(user: String, userRating: scala.collection.Iterable[(String, Float)]): (String, Float) = {
    val list = userRating.toArray
    var usum = 0.0F
    val num = list.size
    for (pair <- 0 until num) usum += list(pair)._2
    (user, usum / num.toFloat)
  }

  def normalize(userRating: (String, (scala.collection.Iterable[(String, Float)], Float))) = {
    val mean = userRating._2._2
    val list = userRating._2._1.toArray.map(li => (li._1, li._2 - mean))
    (userRating._1, list)
  }

  def sampleInteractions(userRating: scala.collection.Iterable[(String, Float)], Interactions: Int): List[(String, Float)] = {
    val userlist = userRating.toList
    if (userlist.length > Interactions) Random.shuffle(userlist).take(Interactions) else userlist
  }

  def calculateCosine(ratingPair: scala.collection.Iterable[(Float, Float)]): Float = {
    var f_xx = 0.0F
    var f_yy = 0.0F
    var f_xy = 0.0F
    for (pair <- ratingPair) {
      f_xx += pair._1 * pair._1
      f_yy += pair._2 * pair._2
      f_xy += pair._1 * pair._2
    }
    val denominator = sqrt(f_xx).toFloat * sqrt(f_yy).toFloat
    if (denominator != 0.0F) Math.round(f_xy * 1000) / 1000F / denominator else 0.0F
  }

  def topN(userRating: scala.collection.Iterable[(String, Float)], number: Int): List[(String, Float)] = {
    userRating.toList.sortWith((x, y) => { math.abs(x._2) > math.abs(y._2) }).take(number)
  }

  def recommenderList(user: String, userSimList: List[(String, Float)], record: scala.collection.immutable.Map[String, Array[(String, Float)]], n: Int): List[String] = {
    val itemMap = Map[String, Float]()
    val denominator = Map[String, Float]()
    userSimList.foreach(user_sim => {
      record(user_sim._1).foreach(score => {
        if (itemMap contains score._1) {
          itemMap(score._1) = itemMap(score._1) + user_sim._2 * score._2
          denominator(score._1) = denominator(score._1) + user_sim._2
        } else {
          itemMap.put(score._1, user_sim._2 * score._2)
          denominator.put(score._1, user_sim._2)
        }
      })
    })

    itemMap.keys.foreach(item => {
      itemMap(item) = Math.round(itemMap(item) / denominator(item) * 1000) / 1000F
    })

    record(user).foreach(record => {
      itemMap remove record._1
    })

    var scoreList = List[String]()
    for (x <- itemMap) {
      scoreList = x._1.toString + ":" + x._2.toString :: scoreList
    }
    scoreList.sortWith(_.split(":")(1).toFloat > _.split(":")(1).toFloat).take(n).map(line => line.split(":")(0))
  }

  def combinations[T](k: Int, list: List[T]): List[List[T]] = {
    list match {
      case Nil => Nil
      case head :: xs =>
        if (k <= 0 || k > list.length) {
          Nil
        } else if (k == 1) {
          list.map(List(_))
        } else {
          combinations(k - 1, xs).map(head :: _) ::: combinations(k, xs)
        }
    }
  }

  def findUserPairs(user_rating: List[(String, Float)]): List[((String, String), (Float, Float))] = {
    val user = user_rating.sortWith(_._1 < _._1)
    combinations(2, user).map(list => ((list(0)._1, list(1)._1), (list(0)._2, list(1)._2)))
  }

}