package spark_algebird

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import twitter4j.Status

object PopularTweets {

  def main(args: Array[String]) {
    val (ssc: StreamingContext, tweets: ReceiverInputDStream[Status]) = initStreamContext("PopularTweets")
    popularTweets(tweets)
    startApp(ssc)
  }

  /**
    * Print top 10 tweets in the stream
    * @param tweets Stream of tweets
    */
  private def popularTweets(tweets: ReceiverInputDStream[Status]) = {
    val statuses = tweets.map(status => status.getText)
    val tweetWords = statuses.flatMap(tweetText => tweetText.split(" "))
    val hashtags = tweetWords.filter(word => word.startsWith("#"))

    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))

    //5 minute window sliding every one second
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy({ case (_, count) => count }, ascending = false))
    sortedResults.print
  }
}