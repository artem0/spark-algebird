# Spark Streaming and Algebird integration app
Spark and Alegebird integration

1) Spark streaming + Tweeter streaming for displaying the most popular tweets
2) Finding the number of unique users via [HLL algorithm](https://en.wikipedia.org/wiki/HyperLogLog) and comparison 
the result of output of HLL to usual algorithm of users count
3) Seeking frequency of users id in single batch and in overall data stream via leveraging [Count-min sketch algorithm](https://en.wikipedia.org/wiki/Count–min_sketch) and comparison 
the result of output of Count-min sketch algorithm to usual algorithm of "word count"
4) Answering the question if collected tweets from batch contains specified word with some probability 
based on [Bloom filter algorithm](https://en.wikipedia.org/wiki/Bloom_filter) algorithm, validity of result which 
was obtained from bloom filter is checking via naive contains method. 
