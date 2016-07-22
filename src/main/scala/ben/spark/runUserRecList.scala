package Ben.com.main

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER
import Ben.com.Util._
import java.net.URI
object runUserRecList {
  def main(args: Array[String]) {

    System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
    
    val knn_number: Int = 10
    val read_coalesce: Int = 200
    val partition_number: Int = 400
    val user_knn_number: Int = 30
    val topN_number: Int = 10
    val user_partition_number: Int = 2000
    val chisquare = 5.024
    val Interactions = 50

    /* Spark configure */
    val conf = new SparkConf().setAppName("userRecList")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext()

    val userData = sc.textFile(StringUtils.DATA_PATH, read_coalesce).map(line => {
      val li = line.split("::")
      (li(0), (li(1), li(2).toFloat))
    }).groupByKey()

    /* 產生用戶推薦清單
       * 以cosine相似度為基準 */

    /* 計算每個用戶的平均評分 */
    val userMeans = userData.map(line => userBasedUtil.calculateMeans(line._1, line._2))

    /* 將每個用戶評分減掉其平均 */
    val dataNormalization = userData.join(userMeans).map(line => userBasedUtil.normalize(line)).cache()

    /* 觀看紀錄需進行廣播(待研究) */
    val userViewOfUB = dataNormalization.map(line => (line._1, line._2.toArray)).collect.toMap
    val broadUserView = sc.broadcast(userViewOfUB)

    /* 以movie為key進行group */
    val itemRecord = dataNormalization.flatMap(line => line._2.map(f => (f._1, (line._1, f._2)))).groupByKey().partitionBy(new HashPartitioner(user_partition_number))
    dataNormalization.unpersist(blocking = false)

    /* 減少計算複雜度 進行抽樣 */
    val randomRecord = itemRecord.map(line => (line._1, userBasedUtil.sampleInteractions(line._2, Interactions)))

    /* 看過同一部影片的用戶pair  */
    val combinationsOfUB = randomRecord.flatMap(line => userBasedUtil.findUserPairs(line._2)).partitionBy(new HashPartitioner(user_partition_number)).groupByKey()

    /* 計算cosine值，並找出最相似的N個用戶 */
    val cosineMatrixUpper = combinationsOfUB.map { line => (line._1._1, (line._1._2, userBasedUtil.calculateCosine(line._2))) }.persist(MEMORY_AND_DISK_SER)
    val cosineMatrixLower = cosineMatrixUpper.map(line => (line._2._1, (line._1, line._2._2)))
    val simMatrix = cosineMatrixLower.union(cosineMatrixUpper).partitionBy(new HashPartitioner(user_partition_number)).groupByKey().map(line => (line._1, userBasedUtil.topN(line._2, user_knn_number)))
    cosineMatrixUpper.unpersist(blocking = true)

    /* 計算前N部影片推單清單，將清單儲存至HDFS */
    simMatrix.map { line => (line._1, userBasedUtil.recommenderList(line._1, line._2, broadUserView.value, topN_number)) }.map(line => {
      var userList = line._1
      val len = line._2.length
      for (li <- line._2) userList = userList + "," + li
      if (len < topN_number) for (i <- 1 to topN_number - len) userList = userList + ","
      userList
    }).saveAsTextFile(StringUtils.HDFS_PATH + StringUtils.USERLIST_OUTPUT_PATH)

  }
}