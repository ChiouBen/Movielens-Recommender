package Ben.com.main

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import Ben.com.Util.StringUtils
import Ben.com.Util.assoUtil

object runAssociationList {
 def main(args: Array[String]) {

    val knn_number: Int = 10
    val read_coalesce: Int = 200
    val partition_number: Int = 400
    val user_knn_number: Int = 30
    val topN_number: Int = 10
    val user_partition_number: Int = 2000
    val chisquare = 5.014
    val Interactions = 50
  
    /* Spark configure */
    val conf = new SparkConf().setAppName("movieRecList")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext()

      /* generate association list 
       * 以卡方獨立性檢定檢定量為判斷相關聯程度: 總用戶數, 影片A被看過次數, 影片B被看過次數, 影片A和B同時被同個user看過的次數 
       * */

    val data = sc.textFile(StringUtils.DATA_PATH, read_coalesce).map(line => {
      val li = line.split("::")
      ((li(0), li(1)), li(2).toFloat)
    }).persist()

    /* 計算每部影片各被觀看過幾次 */
    val itemfrequency = data.map(line => (line._1._2, 1)).reduceByKey(_ + _).collect.toMap
    val broaditemFeq = sc.broadcast(itemfrequency)

    /* 計算總用戶個數 */
    val userSize = data.map(line => line._1._1).distinct().count()
    val broadUserSize = sc.broadcast(userSize)

    /* 計算兩部電影有被同一個用戶看過的次數 */
    val userView = data.map(line => (line._1._1, line._1._2)).groupByKey().partitionBy(new HashPartitioner(partition_number))
    val combinations = userView.flatMap(line => assoUtil.findUserPairs(line._2)).partitionBy(new HashPartitioner(partition_number * 2)).reduceByKey(_ + _)

    /* 合併每部影片的觀看次數並計算其卡方值(上三角矩陣) (a, (b, chi-square value)) */
    val upperChisquare = combinations.map(line => assoUtil.joinItemFequence(broaditemFeq.value, line)).map(line => assoUtil.calculateChisquare(line, broadUserSize.value)).filter(_._2._2 > chisquare).persist()

    /* 合併下三角矩陣並找出最相關聯的N部影片 */
    val knnList = upperChisquare.map(line => (line._2._1, (line._1, line._2._2))).union(upperChisquare).partitionBy(new HashPartitioner(partition_number)).groupByKey().map(line => (line._1, assoUtil.topN(line._2, knn_number)))
    upperChisquare.unpersist()

    /* 將影片關連清單儲存至HDFS */
    knnList.map(line => {
      var itemList = line._1
      val len = line._2.length
      for (li <- line._2) itemList = itemList + ":" + li._1
      if (len < knn_number) for (i <- 1 to knn_number - len) itemList = itemList + ":"
      itemList
    }).saveAsTextFile(StringUtils.HDFS_PATH + StringUtils.ARLIST_OUTPUT_PATH)

 }
}