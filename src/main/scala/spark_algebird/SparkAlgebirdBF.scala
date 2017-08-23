package spark_algebird

import com.twitter.algebird.BloomFilter

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import twitter4j.Status

object SparkAlgebirdBF {

  /**
    * Accuracy for Bloom Filter
    */
  private val BLOOM_FILTER_PROBABILITY = 0.005

  /**
    * Answers the question if collected tweets from batch contains
    * specified word with some probability based on BloomFilter algorithm,
    * validity of result which was obtained from BloomFilter is checking
    * via naive contains method. For the concept of the picture prints total elements
    * for checking and matching of result of BloomFilter and contains method
    * @param tweets Stream of tweets
    */
  def wordFilteringInTweets(tweets: ReceiverInputDStream[Status]): Unit = {
    val tweetsText = tweets.map { _.getText }

    /**
      * Collect tweets during single batch (specified during streaming context initialization)
      */
    val splittedTweetsCollector =
      tweetsText.mapPartitions(_.map(_.split(" "))).reduce(_ ++ _)

    splittedTweetsCollector.foreachRDD { rdd =>
      if (rdd.count() != 0) {

        val splittedTweets: Array[String] = rdd.first()

        /** Necessary for checking result only **/
        val restoredTweets = splittedTweets.mkString(" ")
        val numEntries = splittedTweets.length

        val bloomFilter = BloomFilter(numEntries, BLOOM_FILTER_PROBABILITY)
        val bloomFilterInstance = bloomFilter.create(splittedTweets: _*)

        val wordToCheck = "politic"
        val bloomFilterResult =
          bloomFilterInstance.contains(wordToCheck).isTrue
        val naiveContainsResult = restoredTweets.contains(wordToCheck)
        if (bloomFilterResult != naiveContainsResult) {
          println("Mistake of BloomFilter - possible collision with hash function for omitting this increase accuracy")
        }

        println(
          s"Total words in batch ${splittedTweets.length} " +
            s" BloomFilter result for $wordToCheck --> $bloomFilterResult"
        )
      }
    }
  }
}
