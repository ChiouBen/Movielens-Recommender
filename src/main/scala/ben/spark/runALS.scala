package Ben.com.main

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER
import Ben.com.Util._
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object runALS {
  def main(args: Array[String]) {

    val read_coalesce = 200
    val read_movie_coalesce = 20
    val topN = 10
    
    /* spark context configure */
    val conf = new SparkConf().setAppName("ALSRecList")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext()
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val data = sc.textFile(StringUtils.DATA_PATH, read_coalesce).map(line => {
      val li = line.split("::")
      Rating(li(0).toInt, li(1).toInt, li(2).toFloat)
    })

    val movieData = sc.textFile(StringUtils.MOVIE_DATA_PATH, read_movie_coalesce).map(line => {
      val li = line.split("::")
      (li(0).toInt, li(1))
    }).collect.toMap
    
    /* split train data */
    val data_split = data.randomSplit(Array(0.7, 0.3), 2016)
    val train_data = data_split(0).persist()
    val test_data = data_split(1).persist()

    /* parameter rank:分解維度  ,lambda: 防止overfitting常數 ,numIter: 收斂次數  */
    val ranks = List(10, 15)
    val lambdas = List(0.1, 1, 2)
    val numIters = List(10, 20)

    var bestModel: MatrixFactorizationModel = null
    var minRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1

    /* optimal */
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(train_data, rank, numIter, lambda)
      val vaildationRMse = computeUtils.computeRMSE(model, test_data)
      if (vaildationRMse < minRmse) {
        bestModel = model
        minRmse = minRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }
    println("minRmse:" + minRmse + ",bestRank:" + bestRank + ",bestLambda:" + bestLambda + ",bestNumIter:" + bestNumIter)

    /* get user's recommender list */
    if (bestModel != null) {
      val RecList = computeUtils.getRecList(bestModel, test_data, topN)
    }

    sc.stop()
  }
}