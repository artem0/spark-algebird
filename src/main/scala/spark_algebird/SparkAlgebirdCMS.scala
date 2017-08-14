package spark_algebird

import com.twitter.algebird.{CountMinSketchMonoid, HyperLogLogMonoid, MapMonoid}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status

object SparkAlgebirdCMS {

  def main(args: Array[String]): Unit = {
    val (ssc: StreamingContext, tweets: ReceiverInputDStream[Status]) = initStreamContext("Spark_HLL_Demo", 100)
    userCounter(tweets)
    startApp(ssc)
  }

  /**
    * Get information about amount of users in single batch and overall and via
    * leveraging Count–min sketch algorithm from Algebird and compare it to the usual method
    * @param tweets Stream of tweets
    */
  private def userCounter(tweets: ReceiverInputDStream[Status]) = {
    val DELTA = 1E-10
    val EPS = 0.01
    val SEED = 1
    val PERC = 0.001
    val amountToDisplay = 5
    val users: DStream[Long] = tweets.map(status => status.getUser.getId)

    val cms = new CountMinSketchMonoid(EPS, DELTA, SEED, PERC)
    var globalCMS = cms.zero
    val mm = new MapMonoid[Long, Int]()
    var globalExact = Map[Long, Int]()

    val approxTopUsers = users.mapPartitions(ids => {
      ids.map(id => cms.create(id))
    }).reduce(_ ++ _)

    val exactTopUsers = users.map(id => (id, 1))
      .reduceByKey((a, b) => a + b)

    approxTopUsers.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        val partialTopK = partial.heavyHitters.map(id =>
          (id, partial.frequency(id).estimate)).toSeq.sortBy { case (_, amount) => amount }.reverse.slice(0, amountToDisplay)
        globalCMS ++= partial
        val globalTopK = globalCMS.heavyHitters.map(id =>
          (id, globalCMS.frequency(id).estimate)).toSeq.sortBy { case (_, amount) => amount }.reverse.slice(0, amountToDisplay)
        println("CMS --> Top most valuable users this batch: %s".format(partialTopK.mkString("[", ",", "]")))
        println("CMS --> Top most valuable users overall: %s".format(globalTopK.mkString("[", ",", "]")))
      }
    })

    exactTopUsers.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partialMap = rdd.collect().toMap
        val partialTopK = rdd.sortByKey(ascending = false).take(amountToDisplay)
        globalExact = mm.plus(globalExact, partialMap)
        val globalTopK = globalExact.toSeq.sortBy { case (_, amount) => amount }.reverse.slice(0, amountToDisplay)
        println("Usual --> Top most valuable users in this batch: %s".format(partialTopK.mkString("[", ",", "]")))
        println("Usual --> Top most valuable users overall: %s".format(globalTopK.mkString("[", ",", "]")))
      }
    })
  }
}
