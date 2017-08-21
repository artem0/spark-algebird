package spark_algebird

import com.twitter.algebird.{CMS, CountMinSketchMonoid, MapMonoid}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import twitter4j.Status

object SparkAlgebirdCMS {

  /**
    * CMS instance initializer
    */
  object CountMinSketchMonoidInitializer{
    private val DELTA = 1E-10
    private val EPS = 0.01
    private val SEED = 1
    private val PERC = 0.001

    def initCMS = new CountMinSketchMonoid(EPS, DELTA, SEED, PERC)
  }

  private val COUNT_MIN_SKETCH_MONOID = CountMinSketchMonoidInitializer.initCMS
  private var globalCMS = COUNT_MIN_SKETCH_MONOID.zero

  /**
    * Get information about amount of users in single batch and overall and via
    * leveraging Count–min sketch algorithm from Algebird and compare it to the usual method
    * @param tweets Stream of tweets
    */
  def userCounter(tweets: ReceiverInputDStream[Status]): Unit = {
    val USERS_COUNT = 5
    val users: DStream[Long] = tweets.map(status => status.getUser.getId)

    val approxTopUsers: DStream[CMS] = users.mapPartitions(ids => {
      ids.map(id => COUNT_MIN_SKETCH_MONOID.create(id))
    }).reduce(_ ++ _)

    val exactTopUsers: DStream[(Long, Int)] = users.map(id => (id, 1))
      .reduceByKey(_ + _)

    topTwitterUsersWithCountMinSketch(approxTopUsers, USERS_COUNT)
    exactTopUserInTwitter(exactTopUsers, USERS_COUNT)
  }

  private def topTwitterUsersWithCountMinSketch(approxTopUsers: DStream[CMS], USERS_COUNT: Int) = {
    approxTopUsers.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        val partialTopK = partial.heavyHitters.map(id =>
          (id, partial.frequency(id).estimate)).toSeq.sortBy { case (_, amount) => amount }.reverse.slice(0, USERS_COUNT)
        globalCMS ++= partial
        val globalTopK = globalCMS.heavyHitters.map(id =>
          (id, globalCMS.frequency(id).estimate)).toSeq.sortBy { case (_, amount) => amount }.reverse.slice(0, USERS_COUNT)
        println("CMS --> Top most valuable users this batch: %s".format(partialTopK.mkString("[", ",", "]")))
        println("CMS --> Top most valuable users overall: %s".format(globalTopK.mkString("[", ",", "]")))
      }
    })
  }

  private def exactTopUserInTwitter(exactTopUsers: DStream[(Long, Int)], amountToDisplay:Int) = {
    var globalExact = Map[Long, Int]()
    val mm = new MapMonoid[Long, Int]()
    exactTopUsers.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partialMap = rdd.collect().toMap
        val partialTopK = rdd.sortByKey(ascending = false).take(amountToDisplay)
        globalExact = mm.plus(globalExact, partialMap)
        val globalTopK = globalExact.toSeq.sortBy { case (_, amount) => amount }.reverse.slice(0, amountToDisplay)
        println("Exact --> Top most valuable users in this batch: %s".format(partialTopK.mkString("[", ",", "]")))
        println("Exact --> Top most valuable users overall: %s".format(globalTopK.mkString("[", ",", "]")))
      }
    })
  }
}
