package spark_algebird

import com.twitter.algebird.HyperLogLogMonoid
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import twitter4j.Status

object SparkAlgebirdHLL {

  /**
    * Precision for HLL algoritm, computed as 1.04/sqrt(2^{bits})
    */
  val HLL_PRECISION = 18

  def main(args: Array[String]): Unit = {
    val (ssc: StreamingContext, tweets: ReceiverInputDStream[Status]) = initStreamContext("Spark_HLL_Demo")
    distinctUsers(tweets, HLL_PRECISION)
    startApp(ssc)
  }

  /**
    * Get information about distinct users in single batch and overall and via
    * leveraging HLL algorithm from Algebird and compare it to usual method wia Map-Reduce
    * @param tweets Stream of tweets
    */
  private def distinctUsers(tweets: ReceiverInputDStream[Status], hllPrecision:Int) = {
    val users = tweets.map(status => status.getUser.getId)

    import com.twitter.algebird.HyperLogLog._
    //necessary for implicit conversion Long => Array[Byte]
    val globalHll = new HyperLogLogMonoid(hllPrecision)
    var userSet: Set[Long] = Set()

    val approxUsers = users.mapPartitions(ids => {
      val hll = new HyperLogLogMonoid(hllPrecision)
      ids.map(id => hll(id))
    }).reduce(_ + _)

    val exactUsers = users.map(id => Set(id)).reduce(_ ++ _)

    var h = globalHll.zero
    approxUsers.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        h += partial
        println("HLL --> Amount of users in this batch: %d".format(partial.estimatedSize.toInt))
        println("HLL --> Amount of users overall: %d".format(globalHll.estimateSize(h).toInt))
      }
    })

    exactUsers.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val partial = rdd.first()
        userSet ++= partial
        println("Usual --> Amount of users this batch: %d".format(partial.size))
        println("Usual --> Amount of users overall: %d%n".format(userSet.size))
      }
    })
  }
}
