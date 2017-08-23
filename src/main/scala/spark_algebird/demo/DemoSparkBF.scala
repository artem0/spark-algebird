package spark_algebird.demo

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import spark_algebird.{SparkAlgebirdBF, SparkAlgebirdCMS, initStreamContext, startApp}
import twitter4j.Status

object DemoSparkBF {

  def main(args: Array[String]): Unit = {
    val (ssc: StreamingContext, tweets: ReceiverInputDStream[Status]) = initStreamContext("Spark_HLL_Demo", 120)
    SparkAlgebirdBF.wordFilteringInTweets(tweets)
    startApp(ssc)
  }
}
