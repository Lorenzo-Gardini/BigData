package entities
import java.util.Random

object HashFunctions {
  // generate a seq of numFunctions universal hash functions in form h(x)=((a * x + b) mod p) mod N
  def generateUniversalHashFunctions(numberOfFunctions: Int,
                                     maxValue: Int,
                                     seed: Long): Seq[String => Int] = {
    val random = new Random(seed)
    val MAX_INT_PRIME_LENGTH = 31

    val prime = BigInt.probablePrime(MAX_INT_PRIME_LENGTH, random).toInt

    (1 to numberOfFunctions).map { _ =>
      val a = random.nextInt(prime - 1) + 1 // a should be > 0
      val b = random.nextInt(prime) // b can be = 0
      x: String => {
        val hashValue = ((a.toLong * x.hashCode + b) % prime).toInt % maxValue
        if (hashValue < 0) hashValue + maxValue else hashValue
      }
    }
  }
}
