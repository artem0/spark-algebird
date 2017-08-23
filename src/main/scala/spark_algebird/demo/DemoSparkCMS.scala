package spark_algebird.demo

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import spark_algebird.{initStreamContext, startApp, SparkAlgebirdCMS}
import twitter4j.Status

object DemoSparkCMS {

  def main(args: Array[String]): Unit = {
    val (ssc: StreamingContext, tweets: ReceiverInputDStream[Status]) =
      initStreamContext("Spark_HLL_Demo", 100)
    SparkAlgebirdCMS.userCounter(tweets)
    startApp(ssc)
  }
}
