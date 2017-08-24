package algebird

import com.twitter.algebird._

object AlgebirdProbabilisticAlgorithmSample {

  /**
    * if increase precision to 10, estimate will almost as actualSize
    */
  def sampleHLL()= {
    import com.twitter.algebird.HyperLogLog._
    val hll = new HyperLogLogMonoid(4)
    val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
    val seqHll = data.map {hll(_)}
    val sumHll = hll.sum(seqHll)
    val approxSizeOf = hll.sizeOf(sumHll)
    val actualSize = data.toSet.size
    val estimate = approxSizeOf.estimate
    println(s"actual - $actualSize and estimate - $estimate")
  }

  def sampleBloomFilter(element: String) = {
    val NUM_HASHES = 6
    val WIDTH = 32
    val SEED = 1
    val bfMonoid = BloomFilterMonoid(NUM_HASHES, WIDTH, SEED)
    val bf = bfMonoid.create("1", "2", "3", "4", "100")
    val approxBool = bf.contains(element)
    approxBool.isTrue
  }

  def sampleCMS() = {
    val DELTA = 1E-10
    val EPS = 0.001
    val SEED = 1
    val CMS_MONOID = new CountMinSketchMonoid(EPS, DELTA, SEED)
    val data = Seq(1L, 1L, 3L, 4L, 5L)

    val cms = CMS_MONOID.create(data)
    cms.totalCount
    cms.frequency(1L).estimate
    cms.frequency(2L).estimate
    cms.frequency(3L).estimate

    cms.frequency(1L).estimate
  }

  def sampleMinHashLSH[T](x: Set[T], y: Set[T]) =
    approxSimilarity(new MinHasher32(0.5, 1024), x, y)


  def approxSimilarity[T, H](mh: MinHasher[H], x: Set[T], y: Set[T]): Double = {
    val sig1 = x.map{ l => mh.init(l.toString) }.reduce{ (a, b) => mh.plus(a, b) }
    val sig2 = y.map{ l => mh.init(l.toString) }.reduce{ (a, b) => mh.plus(a, b) }
    mh.similarity(sig1, sig2)
  }

  /**
    * Helper method for generation two random sets with specified similarity
    */
  def randomSets(similarity: Double): (Set[Double], Set[Double]) = {
    val s = 10000
    val uniqueFraction = if (similarity == 1.0) 0.0 else (1 - similarity) / (1 + similarity)
    val sharedFraction = 1 - uniqueFraction
    val unique1 = 1.to((s * uniqueFraction).toInt).map{ i => math.random }.toSet
    val unique2 = 1.to((s * uniqueFraction).toInt).map{ i => math.random }.toSet

    val shared = 1.to((s * sharedFraction).toInt).map{ i => math.random }.toSet
    (unique1 ++ shared, unique2 ++ shared)
  }
}
