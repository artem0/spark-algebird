package algebird

object QTreeSample {

  def main(args: Array[String]): Unit = {
    quantilesQTreeString(List((1L, "one"), (2L, "two"), (3L, "three"), (4L, "four"), (5L, "five"), (6L, "six")))
    quantilesQTree(List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8))
    quantilesQTree(1 to 32 toList)
  }

  private def quantilesQTree(data:List[Int]) = {
    import com.twitter.algebird._
    val seqQTree = data.map(p => QTree(p))
    val qtSemigroup = new QTreeSemigroup[Long](6)
    val sum = qtSemigroup.sumOption(seqQTree).get

    println("count " + sum.count)

    println("lowerChild " + sum.lowerChild)
    println("upperChild " + sum.upperChild)
    println("lower " + sum.lowerBound)
    println("upper " + sum.upperBound)
    println("0.25 --> " + sum.quantileBounds(0.25))
    println("0.5  --> " + sum.quantileBounds(0.5))
    println("0.75 --> " + sum.quantileBounds(0.75))
  }

  private def quantilesQTreeString(data:List[(Long, String)]) = {
    import com.twitter.algebird._
    val seqQTree = data.map(p => QTree(p))
    val qtSemigroup = new QTreeSemigroup[String](6)
    val sum = qtSemigroup.sumOption(seqQTree).get

    println("count " + sum.count)

    println("lowerChild " + sum.lowerChild)
    println("upperChild " + sum.upperChild)
    println("lower " + sum.lowerBound)
    println("upper " + sum.upperBound)
    println("0.25 --> " + sum.quantileBounds(0.25))
    println("0.5  --> " + sum.quantileBounds(0.5))
    println("0.75 --> " + sum.quantileBounds(0.75))
  }

}
