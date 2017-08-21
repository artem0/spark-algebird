# Spark Streaming and Algebird integration app
Spark and Alegebird integration

1) Spark streaming + Tweeter streaming for displaying the most popular tweets
2) Spark streaming + Tweeter streaming + [Algebird](https://github.com/twitter/algebird) for searching amount of 
unique users via [HLL algorithm](https://en.wikipedia.org/wiki/HyperLogLog) and comparison 
the result of output of HLL to usual algorithm of users count
3) Spark streaming + Tweeter streaming + [Algebird](https://github.com/twitter/algebird) for searching information 
about amount of users in single batch and overall via leveraging [Count-min sketch algorithm](https://en.wikipedia.org/wiki/Count–min_sketch) and comparison 
the result of output of Count-min sketch algorithm to usual algorithm of "word count"
