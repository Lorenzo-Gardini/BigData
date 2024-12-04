package entities

import java.util.{Collections, Random}

trait BloomFilter {
  def contains(id: String): Boolean
  def +(id: String): BloomFilter
  def |(other: BloomFilter): BloomFilter
  def memory: Array[Byte]
}

object BloomFilter{
  private case class BloomFilterImpl(var internalMemory: Array[Byte],
                                     hashFunctions: Seq[String => Int]) extends BloomFilter{
    override def memory: Array[Byte] = internalMemory.clone()
    override def +(id: String): BloomFilter = {
      hashFunctions.map(f => f(id)).foreach { position =>
        val (reminder, memoryIndex) = reminderAndMemoryIndex(position)
        internalMemory(memoryIndex) = (internalMemory(memoryIndex) | (1 << reminder).toByte).toByte
      }
      this
    }

    private def reminderAndMemoryIndex(position: Int): (Int, Int) = (position % 8, position / 8)

    override def |(other: BloomFilter): BloomFilter =
      BloomFilterImpl(internalMemory.zip(other.memory).map(c => (c._1 | c._2).toByte), hashFunctions)

    override def contains(id: String): Boolean = !hashFunctions.map(f => f(id)).map { position =>
      val (reminder, memoryIndex) = reminderAndMemoryIndex(position)
      (internalMemory(memoryIndex) & (1 << reminder).toByte).toByte
    }.contains(0)
  }

  def apply(bitmapLength: Int, hashFunctions: Seq[String => Int]): BloomFilter = {
    val memorySize = math.ceil((bitmapLength + 1) / 8.doubleValue).toInt
    val emptyMemory: Array[Byte] = Array.fill(memorySize)(0)
    BloomFilterImpl(emptyMemory, hashFunctions)
  }

  def bitmapLength(probabilityOfFalsePositive: Double,
                   numberExpectedElements: Int): Int = {
    val lnP = math.log(probabilityOfFalsePositive)
    val ln2 = math.log(2)
    math.ceil(-numberExpectedElements * lnP / math.pow(ln2, 2)).toInt
  }
  def generateUniversalHashFunctions(probabilityOfFalsePositive: Double,
                                     bitmapLength: Int,
                                     seed: Long): Seq[String => Int] = {
    val lnP = math.log(probabilityOfFalsePositive)
    val ln2 = math.log(2)
    val hashFunctionsNumber: Int = math.ceil(-lnP / ln2).toInt // = -log2(x)

    HashFunctions.generateUniversalHashFunctions(hashFunctionsNumber, bitmapLength, seed)
  }
}