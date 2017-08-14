import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status

package object spark_algebird {

  private def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  private def setupTwitter(): Unit = {
    import scala.io.Source

    for (line <- Source.fromFile("twitter.conf").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

  /**
    * Path to checkpoint directory
    */
  private val CHECKPOINT_PATH = "checkpoint"

  /**
    * Launch app and set up checkpoint
    * @param ssc StreamingContext
    */
  def startApp(ssc: StreamingContext): Unit = {
    ssc.checkpoint(CHECKPOINT_PATH)
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * Get StreamingContext and ReceiverInputDStream for streaming tweets
    * @param appName application name
    * @return the tuple of StreamingContext ReceiverInputDStream
    */
  def initStreamContext(appName: String): (StreamingContext, ReceiverInputDStream[Status]) = {
    setupTwitter()
    val ssc = new StreamingContext("local[*]", appName, Seconds(1))
    setupLogging()
    val tweets = TwitterUtils.createStream(ssc, None)
    (ssc, tweets)
  }
}
