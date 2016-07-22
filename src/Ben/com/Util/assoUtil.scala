package Ben.com.Util

import java.util.Calendar
import java.text.SimpleDateFormat
import Ben.com.Util._

/**
 * @author BenChiou
 */
@transient
object assoUtil extends Serializable{

  def findUserPairs(userRating: scala.collection.Iterable[String]): List[((String, String), Int)] = {
    userRating.toList.sortWith(_.toFloat < _.toFloat).toSet.subsets(2).map(_.toList).toList.map(list => ((list(0), list(1)), 1))
  }

  def calculateChisquare(itemPair: ((String, String), (Int, Int, Int)), size: Long): (String, (String, Double)) = {
    val p2ab = itemPair._2._1.toFloat / size
    val p2a = itemPair._2._2.toFloat / size
    val p2b = itemPair._2._3.toFloat / size
    val conf = p2ab / p2a
    val lift = p2ab / (p2a * p2b)
    (itemPair._1._1, (itemPair._1._2, size * math.pow((lift - 1), 2) * p2ab * conf / ((conf - p2ab) * (lift - conf))))
  }

  def topN(itemChiValue: scala.collection.Iterable[(String, Double)], number: Int): List[(String, Double)] = {
    itemChiValue.toList.sortWith((x, y) => { x._2 > y._2 }).take(number)
  }

  def joinItemFequence(itemFeq: scala.collection.immutable.Map[String, Int], itemPair: ((String, String), Int)): ((String, String), (Int, Int, Int)) = {
    (itemFeq(itemPair._1._1), itemFeq(itemPair._1._2))
    (itemPair._1, (itemPair._2, itemFeq(itemPair._1._1), itemFeq(itemPair._1._2)))
  }
  
}