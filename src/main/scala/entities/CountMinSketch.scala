package entities

import java.util.Random

trait CountMinSketch {

  def +(id: String): CountMinSketch
  def getCount(id: String): Int
  def ++(other: CountMinSketch): CountMinSketch
  def counters: Array[Array[Int]]
}


object CountMinSketch{
  private case class CountMinSketchImpl(var internalCounters: Array[Array[Int]],
                                        hashFunctions: Seq[String => Int]) extends CountMinSketch{
    override def +(id: String): CountMinSketch = {
      hashFunctions
      .map(f => f(id))
      .zipWithIndex
      .foreach{ case (i, fnPos) => internalCounters(fnPos)(i) += 1}
      this
    }

    override def getCount(id: String): Int =
      hashFunctions
        .map(f => f(id))
        .zipWithIndex
        .map{ case (i, fnPos) => internalCounters(fnPos)(i)}
        .min

    override def ++(other: CountMinSketch): CountMinSketch =
      CountMinSketchImpl(internalCounters
        .zip(other.counters)
        .map{ case (l1, l2) => l1.zip(l2).map{ case (v1, v2) => v1 + v2}},
        hashFunctions)

    override def counters: Array[Array[Int]] = internalCounters.clone()
  }

  def apply(countersLength: Int, hashFunctions: Seq[String => Int]): CountMinSketch = {
    CountMinSketchImpl(
      Array.fill(hashFunctions.length)(Array.fill(countersLength)(0)): Array[Array[Int]],
      hashFunctions
    )
  }
  def generateUniversalHashFunctions(errorProbability: Double,
                                     countersLength: Int,
                                     seed: Long): Seq[String => Int] = {
    val invP = math.log(1/errorProbability)
    val hashFunctionsNumber: Int = math.ceil(invP).toInt // = ceil(1/ln(x))
    HashFunctions.generateUniversalHashFunctions(hashFunctionsNumber, countersLength, seed)
  }

  def countersLength(error: Double): Int = math.ceil(math.E/error).toInt
}
