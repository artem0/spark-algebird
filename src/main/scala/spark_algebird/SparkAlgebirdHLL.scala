package spark_algebird

import com.twitter.algebird.HyperLogLog._
import com.twitter.algebird.{HLL, HyperLogLogMonoid}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import twitter4j.Status //necessary for implicit conversion Long => Array[Byte]

object SparkAlgebirdHLL {

  /**
    * Precision for HLL algoritm, computed as 1.04/sqrt(2^{bits})
    */
  private val HLL_PRECISION = 18

  /**
    * Global HLL instance
    */
  private val HYPER_LOG_LOG_MONOID = new HyperLogLogMonoid(HLL_PRECISION)

  /**
    * Get information about distinct users in single batch and overall and via
    * leveraging HLL algorithm from Algebird and compare it to usual method wia Map-Reduce
    * @param tweets Stream of tweets
    */
  def distinctUsers(tweets: ReceiverInputDStream[Status]): Unit = {
    val users = tweets.map(status => status.getUser.getId)

    val approxUsers: DStream[HLL] = users
      .mapPartitions(ids => {
        val hll = new HyperLogLogMonoid(HLL_PRECISION)
        ids.map(id => hll(id))
      })
      .reduce(_ + _)

    val exactUsers: DStream[Set[Long]] =
      users.map(id => Set(id)).reduce(_ ++ _)

    approximateUserCountHLL(approxUsers)
    exactUserCount(exactUsers)
  }

  private def exactUserCount(exactUsers: DStream[Set[Long]]) = {
    var userSet: Set[Long] = Set()
    exactUsers.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        userSet ++= partial
        println("Exact --> Amount of users this batch: %d".format(partial.size))
        println("Exact --> Amount of users overall: %d%n".format(userSet.size))
      }
    })
  }

  private def approximateUserCountHLL(approxUsers: DStream[HLL]) = {
    var h = HYPER_LOG_LOG_MONOID.zero
    approxUsers.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        h += partial
        println("HLL --> Amount of users in this batch: %d".format(partial.estimatedSize.toInt))
        println("HLL --> Amount of users overall: %d".format(HYPER_LOG_LOG_MONOID.estimateSize(h).toInt))
      }
    })
  }
}
