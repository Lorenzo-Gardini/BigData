import com.github.tototoshi.csv._
import entities._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.Date
import scala.collection.immutable.BitSet
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.Using
import scala.util.hashing.MurmurHash3


object Main{

  private val DefaultStorageLevel = StorageLevel.DISK_ONLY
  private val CacheStorage = "C:\\Users\\lo.gardini\\Desktop\\Local\\personal projects\\test\\2022_place_canvas_history.csv\\cache"
  private val SerializedFile = "C:\\Users\\lo.gardini\\Desktop\\Local\\personal projects\\test\\2022_place_canvas_history.csv\\serialized"
  private val LogDir = "C:\\Users\\lo.gardini\\Desktop\\Local\\personal projects\\test\\2022_place_canvas_history.csv\\logs"
  private val Seed = 1234

  private def computeTime(durationInSeconds: Double): Unit = {
    val minutes = (durationInSeconds / 60).toInt
    val seconds = (durationInSeconds % 60).toInt
    println(f"Execution time: $minutes:$seconds%02d")
  }

  private def getFileSize(path: String): Long = new File(path).length()

  private def calculatePartitions(filePath: String, partitionSizeMB: Int): Int =
    Math.ceil(getFileSize(filePath).toDouble / (partitionSizeMB * 1024 * 1024)).toInt

  private def decorateExecution[R](jobName: String, fn: => R): R = {
    println(s"######### $jobName #########")
    val startTime = System.currentTimeMillis()
    val result = fn
    computeTime((System.currentTimeMillis() - startTime) / 1000.0)
    result
  }

  private def clearCacheFolder(): Unit =
    Files.list(Paths.get(CacheStorage))
      .iterator()
      .asScala
      .foreach { path =>
        if (Files.isDirectory(path)) {
          Files.walk(path)
            .iterator()
            .asScala
            .toSeq
            .reverse
            .foreach(Files.delete)
        }
        else{
          Files.delete(path)
        }
      }

  private def writeToCSV(header: Seq[String], values: Seq[Seq[Any]], filePath: String): Unit =
    Using.resource(CSVWriter.open(filePath)) { writer =>
      writer.writeRow(header)
      values.foreach(v => writer.writeRow(v))
    }

  def main(args: Array[String]): Unit = {
    clearCacheFolder()
    val spark = SparkSession.builder()
      .appName("Test")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "localhost")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.logBlockUpdates.enabled", "true")
      .config("spark.eventLog.dir", LogDir)
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.local.dir", CacheStorage)
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val startTime = System.currentTimeMillis()
    app(spark.sparkContext)
    computeTime((System.currentTimeMillis() - startTime) / 1000.0)

    spark.stop()
  }

  private def filterOnlyTilePlacement[T: ClassTag](df: RDD[Placement], map_func: TilePlacement => T): RDD[T] =
    df.flatMap{
      case tilePlacement: TilePlacement => Some(map_func(tilePlacement))
      case _ => None
    }

  private def getThirds(values: Seq[Int]): (Int, Int) = getThirds(values.min, values.max)

  private def getThirds(minValue: Int, maxValue: Int): (Int, Int) = {
    val rangeValue = maxValue - minValue
    val firstThird = minValue + rangeValue * 1 / 3
    val secondoThird = minValue + rangeValue * 2 / 3
    (firstThird, secondoThird)
  }

  /** Most changed tiles.
   * Optimization is TakeOrdered instead of sortBy() followed by take */
  private def mostChangedTiles(df: RDD[Placement]): Unit = {
    val colors = filterOnlyTilePlacement(df, placement => placement.coordinate -> 1)
      .reduceByKey(_ + _)
      .takeOrdered(5)(Ordering.by(-_._2))
    println(colors.mkString("\n"))
  }

  /** Recreate tile status of areas changed by each admin interventions.
   *  Optimizations are
   *  - change DateTime to log (millis)
   *  - the use of broadcast variable for retrieved admin placements */
  private def recreateAdminInterventions(df: RDD[Placement], sparkContext: SparkContext): Unit = {
    def isInArea(singleCoordinate: SingleCoordinate, rectangleCoordinate: RectangleCoordinate): Boolean =
      math.min(rectangleCoordinate.x1, rectangleCoordinate.x2) <= singleCoordinate.x &&
        singleCoordinate.x <= math.max(rectangleCoordinate.x1, rectangleCoordinate.x2) &&
        math.min(rectangleCoordinate.y1, rectangleCoordinate.y2) <= singleCoordinate.y &&
        singleCoordinate.y <= math.max(rectangleCoordinate.y1, rectangleCoordinate.y2)

    def getOptionAdmin(tilePlacement: TilePlacement, areaPlacements: Seq[AreaPlacement]): Option[AreaPlacement] =
      areaPlacements
        .filter(p => isInArea(tilePlacement.coordinate, p.coordinate) && p.dateTime > tilePlacement.dateTime)
        .minByOption(_.dateTime)

    val adminTiles = sparkContext.broadcast(
      df.flatMap {
        case p: AreaPlacement => Some(p)
        case _ => None
      }.collect())

    val userTiles = df
      .flatMap {
        case t: TilePlacement => getOptionAdmin(t, adminTiles.value).map(a => (a, t.coordinate) -> (t.dateTime, t.pixelColor))
        case _ => None
      }
      .reduceByKey((a, b) => if (a._1 > b._1) a else b)
      .map {
        case ((areaPlacement, singleCoordinate), (_, color)) =>
          (areaPlacement.userId, singleCoordinate.x, singleCoordinate.y, color)
      }
      .collect()

//    val writer = new PrintWriter(new File("back_in_time.csv"))
//    writer.write("id,x,y,color\n")
//    userTiles.toSeq.foreach {
//      case (userId, x, y, pixelColor) =>
//        writer.write(s"$userId,$x,$y,$pixelColor\n")
//    }
//    writer.close()
  }

  /** Each tile is classified in BadSpot, AverageSpot or GoodSpot based on the number of placement in that coordinate
   * compared to the lowest and the higher (the interval divided in 3 parts). For each class, the most used color is
   * then found.
   * Optimizations are:
   * - coordinate to frequency is collected (the total number of coordinates are pow(2000, 2) = 4000000 which is )
   * - the use of broadcast variable for retrieved admin placements
   */
  private def mostPresentColorPerClass(df: RDD[Placement], sparkContext: SparkContext): Unit = {

    val coordinateToPixelColor = filterOnlyTilePlacement(df, placement => placement.coordinate -> placement.pixelColor)
    val coordinateToFrequency = filterOnlyTilePlacement(df, placement => placement.coordinate -> 1)
      .reduceByKey(_ + _)
      .collectAsMap()

    val (firstThird, secondThird) = getThirds(coordinateToFrequency.values.toSeq)

    val coordinateToQuality = sparkContext.broadcast(coordinateToFrequency.map {
      case (coordinate, freq) => coordinate -> (if (freq < firstThird) BadSpot else if (freq < secondThird) AverageSpot else GoodSpot)
    })

    val qualityToColor = coordinateToPixelColor
      .flatMap {
        case (coordinates, pixelColor) =>
          coordinateToQuality.value.get(coordinates).map { quality => ((quality, pixelColor), 1) }
      }
      .reduceByKey(_ + _)
      .map { case ((quality, color), count) => quality -> (color, count) }
      .reduceByKey((c1, c2) => if (c1._2 > c2._2) c1 else c2)
    println(qualityToColor.collect().mkString("\n"))
  }

  /** First version of mostPresentColorPerClass without optimizations */
  private def mostPresentColorPerClassOld(df: RDD[Placement]): Unit = {

    val coordinateToPixelColor = filterOnlyTilePlacement(df, placement => placement.coordinate -> placement.pixelColor)
    val coordinateToFrequency = filterOnlyTilePlacement(df, placement => placement.coordinate -> 1)
      .reduceByKey(_ + _)

    val (firstThird, secondThird) = getThirds(coordinateToFrequency.map(_._2).min(), coordinateToFrequency.map(_._2).max())

    val coordinateToQuality = coordinateToFrequency.map {
      case (coordinate, freq) => coordinate -> (if (freq < firstThird) BadSpot else if (freq < secondThird) AverageSpot else GoodSpot)
    }

    val qualityToColor = coordinateToPixelColor.join(coordinateToQuality)
      .map { case (_, (color, quality)) => (quality, color) -> 1}
      .reduceByKey(_ + _)
      .map { case ((quality, color), count) => quality -> (color, count) }
      .reduceByKey((c1, c2) => if (c1._2 > c2._2) c1 else c2)
    println(qualityToColor.collect().mkString("\n"))
  }

  /** Duration between first and last placement for each user if he/she has multiple placements
   * Optimizations are:
   * - time to millis instead of DateTime
   * - max and min time for each user computed at the same time in aggregateByKey
   * - takeOrdered instead sortBy and take
   * */
  private def retentionUsers(df: RDD[Placement]): Unit = {
    def seqOp(acc: (Long, Long), value: Long): (Long, Long) = {
      val (minTime, maxTime) = acc
      (math.min(minTime, value), math.max(maxTime, value))
    }

    def combOp(acc1: (Long, Long), acc2: (Long, Long)): (Long, Long) = {
      val (minTime1, maxTime1) = acc1
      val (minTime2, maxTime2) = acc2
      (math.min(minTime1, minTime2), math.max(maxTime1, maxTime2))
    }

    def millisToDhms(milliseconds: Long): (Int, Int, Int, Int) = {
      val seconds = milliseconds / 1000
      val days = (seconds / 86400).toInt
      val hours = ((seconds % 86400) / 3600).toInt
      val minutes = ((seconds % 3600) / 60).toInt
      val secondsResult = (seconds % 60).toInt
      (days, hours, minutes, secondsResult)
    }

    val maxDateTime = new Date(Long.MaxValue).toInstant.toEpochMilli
    val minDateTime = new Date(Long.MinValue).toInstant.toEpochMilli

    val userToDateTime = filterOnlyTilePlacement(df, placement => placement.userId -> placement.dateTime)
      .aggregateByKey((maxDateTime, minDateTime))(seqOp, combOp)
      .flatMap{
        case (user, (firstPlacement, lastPlacement)) if lastPlacement - firstPlacement > 0 =>
          Some(user -> (lastPlacement - firstPlacement))
        case _ => None
      }
      .takeOrdered(5)(Ordering.by(-_._2))
      .map{ u =>
        val (days, hours, minutes, seconds) = millisToDhms(u._2)
        u._1 -> s"$days days $hours:$minutes:$seconds"
      }

    println(userToDateTime.mkString("\n"))
  }

  /** Max Manhattan distance covered by each user
   * Optimizations are:
   * - use of ArrayBuffer for faster appending mechanism
   * - user of placements.sizeIs to avoid user with one placement
   * - sort directly after aggregateByKey
   * - takeOrdered instead sortBy and take
   */
  private def maxManhattanDistance(df: RDD[Placement]): Unit = {
    def manhattanDistance(p1: SingleCoordinate, p2: SingleCoordinate): Int = math.abs(p1.x - p2.x) + math.abs(p1.y - p2.y)
    val distances = filterOnlyTilePlacement(df, placement => placement.userId -> (placement.dateTime, placement.coordinate))
      .aggregateByKey(ArrayBuffer[(Long, SingleCoordinate)]())(
        (acc, value) => { acc += value; acc },
        (acc1, acc2) => { acc1 ++= acc2; acc1 }
      )
      .flatMap {
        case (id, placements) if placements.sizeIs > 1 =>
          Some(id ->
            placements.sortBy(_._1)
              .map(_._2)
              .sliding(2)
              .map(window => manhattanDistance(window(0), window(1)))
              .sum)
        case _ => None
      }
      .takeOrdered(5)(Ordering.by(-_._2))

    println(distances.mkString("\n"))
  }

  /** Computation of some metrics for dataset analysis
   * Optimizations are:
   * - use of persist because AreaPlacements are used multiple times
   */
  private def numbers(df: RDD[Placement]): Long = {
    val numberOfUsers = filterOnlyTilePlacement(df, placement => placement.userId).distinct().count()
    val areaPlacements = df.flatMap{
      case area: AreaPlacement => Some(area)
      case _ => None
    }.persist(DefaultStorageLevel)

    println(s"""Number of placements: ${df.count()},\nNumber of distinct users: $numberOfUsers,\nNumber of distinct admins ${areaPlacements.map(_.userId).distinct().count()},\nNumber of admin intervention ${areaPlacements.count()}""")
    numberOfUsers
  }

  /** Computation of approximate users number using HyperLogLog algorithm
   * Optimizations are:
   * - use of binary operations for speed increase
   * */
  private def hyperLogLog(df: RDD[String], nDigits: Int): Int = {
    def countZeros(number: Int): Int = {
      if (number == 0) Integer.SIZE else
      Integer.toBinaryString(number)
        .reverse
        .takeWhile(_ == '0')
        .length
    }
    def computeOnValue(id: String, nDigits: Int): (Int, Int) = {
      val hash = MurmurHash3.stringHash(id)
      val bucketNumber = hash & ((1 << nDigits) - 1)
      (bucketNumber, countZeros(hash >>> nDigits))
    }
    val nBuckets = 2 << nDigits
    val Z = df.map{ id =>
        val (bucketNumber, zeroCount) = computeOnValue(id, nDigits = nDigits)
        bucketNumber -> zeroCount
      }
      .reduceByKey((x,y) => math.max(x,y))
      .map(e => Math.pow(2, -e._2))
      .sum()

    val alphaMM = 0.7213 / (1 + 1.079 / nBuckets) // Correction constant
    val approxValue = (alphaMM * math.pow(nBuckets, 2) / Z).round.toInt
    approxValue
  }

  /** Computation of most active user (the one who placed more tiles)
   * Optimization are:
   * - takeOrdered instead of orderBy and take */
  private def mostActiveUser(df: RDD[Placement]): (String, Int) = {
    val (mostActiveUserId, placements) = df.map(_.userId -> 1)
      .reduceByKey(_ + _)
      .takeOrdered(1)(Ordering[Int].reverse.on(_._2))
      .head
    println(s"Most active user is $mostActiveUserId with $placements placements")
    (mostActiveUserId, placements)
  }

  /** Builds a Count-Min Sketch from an RDD of strings, given an error probability,
   * error tolerance, and hash seed. It calculates the optimal counters size and hash functions,
   * then aggregates elements within partitions (+) and across partitions (++) to create
   * and return the final Count-Min Sketch.
   * Optimization are:
   * - custom data structure CountMinSketch for counters management */
  private def createCountMinSketch(df: RDD[String],
                                   errorProbability: Double,
                                   error: Double,
                                   seed: Long): CountMinSketch = {
    val countersLength = CountMinSketch.countersLength(error)
    val hashFunctions = CountMinSketch.generateUniversalHashFunctions(errorProbability, countersLength, seed)
    df.aggregate(CountMinSketch(countersLength, hashFunctions))(
      (acc, id) => acc + id,
      (acc1, acc2) => acc1 ++ acc2
    )
  }

  /** Generates a Bloom Filter from an RDD of strings,
   * using a specified false-positive probability, expected element count, and hash seed.
   * It calculates the optimal bitmap size and hash functions, then aggregates elements
   * within partitions (+) and across partitions (|) to build and return the final filter.
   * Optimization are:
   * - custom data structure BloomFilter for bitmap management */
  private def createBloomFilter(df: RDD[String],
                                probabilityOfFalsePositive: Double,
                                numberExpectedElements: Int,
                                seed: Long): BloomFilter = {
    val bitmapLength = BloomFilter.bitmapLength(probabilityOfFalsePositive, numberExpectedElements)
    val hashFunctions = BloomFilter.generateUniversalHashFunctions(probabilityOfFalsePositive, bitmapLength, seed)
    df.aggregate(BloomFilter(bitmapLength, hashFunctions))(
        (acc, id) => acc + id,
        (acc1, acc2) => acc1 | acc2
      )
  }


  /**
   * Calculates the distribution of quality for coordinates and users based on placement data.
   *
   * 1. Coordinates are classified into three categories (BadSpot, AverageSpot, GoodSpot) based on their frequency.
   * 2. Users are classified into three categories (LowActivityUser, ModeratelyActiveUser, HighlyActiveUser)
   *    based on the number of placements they have made.
   * 3. A pairing of user quality and coordinate quality is calculated, considering the quality of the most changed coordinate
   *    by each user.
   * Optimizations are:
   * - use of persist for RDD reuse
   * - map transformations avoided before join
   * - user and coordinate class mapping applied after join
   */
  private def qualityDistribution(df: RDD[Placement], sparkContext: SparkContext): Unit = {
    // COORDINATES
    val coordinateToFrequency = filterOnlyTilePlacement(df, placement => placement.coordinate -> 1)
      .reduceByKey(_ + _)
      .collectAsMap()

    val (coordinateFirstThird, coordinateSecondThird) = getThirds(coordinateToFrequency.values.toSeq)

    val coordinateToClass = sparkContext.broadcast(coordinateToFrequency.map{
      case (coordinate, freq) => coordinate ->
        (if (freq < coordinateFirstThird) BadSpot
        else if (freq < coordinateSecondThird) AverageSpot
        else GoodSpot)
    })

    // USER
    val userToMostChangedCoordinate =
      filterOnlyTilePlacement(df, placement => (placement.userId, placement.coordinate) -> 1)
        .reduceByKey(_ + _)
        .map{ case ((userId, coordinate), count) => userId -> (coordinate, count) }
        .reduceByKey { (a, b) =>  if (a._2 >= b._2) a else b }

    val userToPlacementNumber = filterOnlyTilePlacement(df, placement => placement.userId -> 1)
      .reduceByKey(_ + _)
      .persist(DefaultStorageLevel)

    val (userFirstThird, userSecondThird) = getThirds(userToPlacementNumber.values.min(), userToPlacementNumber.values.max())

    val userQualityToCoordinateQuality = userToPlacementNumber.join(userToMostChangedCoordinate)
      .flatMap {
        case (_, (userPlacement, (coordinate, _))) =>
          val userClass = if (userPlacement < userFirstThird) LowActivityUser else if (userPlacement < userSecondThird) ModeratelyActiveUser else HighlyActiveUser
          coordinateToClass.value.get(coordinate).map(coordinateClass => (userClass, coordinateClass) -> 1)}
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .collect()

    println(userQualityToCoordinateQuality.mkString("\n"))
  }


  private def app(sparkContext: SparkContext): Unit = {
    val largeDataset = "C:\\Users\\lo.gardini\\Desktop\\Local\\personal projects\\test\\2022_place_canvas_history.csv\\2022_place_canvas_history.csv"
    val df = sparkContext.textFile(largeDataset)
      .coalesce(calculatePartitions(largeDataset, partitionSizeMB = 256))
      .flatMap(r => Placement(r))
      .persist(DefaultStorageLevel)
    df.count()
//    df.saveAsObjectFile(SerializedFile)
//    val df = sparkContext.objectFile[Placement](SerializedFile)

    val realNumberOfUsers: Long = decorateExecution("Numbers", numbers(df))
    val (mostActiveUserId, placements) = decorateExecution("Most active user", mostActiveUser(df))

    decorateExecution("Most changed tiles", mostChangedTiles(df))
    decorateExecution("Max manhattan distance", maxManhattanDistance(df))
    decorateExecution("Retention users", retentionUsers(df))
    decorateExecution("Recreate admin interventions", recreateAdminInterventions(df, sparkContext))
    decorateExecution("Most present color per class", mostPresentColorPerClass(df, sparkContext))
    decorateExecution("Most present color per class old", mostPresentColorPerClassOld(df))
    decorateExecution("Quality distribution", qualityDistribution(df, sparkContext))

    // ALGORITHMS
    val placementSample = df.sample(withReplacement = false, fraction = 0.1, seed = 1234)
    val approxNumberOfUsers: Int = decorateExecution(s"Number of users HyperLogLog",
      hyperLogLog(filterOnlyTilePlacement(placementSample, _.userId), nDigits = 13))
    println(s"Real number of users: $realNumberOfUsers, approximated number of users: $approxNumberOfUsers\n" +
      s"User number error: ${math.abs(realNumberOfUsers - approxNumberOfUsers).doubleValue / realNumberOfUsers.doubleValue * 100}%")

    val bloomFilterCoalescePartitionsNumber = 4 // 240 MB of bloom filter * 5 partitions = 1200 MB > max driver size
    val bloomFilter: BloomFilter = decorateExecution("Create Bloom Filter",
      createBloomFilter(df.map(_.userId).coalesce(bloomFilterCoalescePartitionsNumber),
        probabilityOfFalsePositive = 0.01, // = 7 hash functions
        numberExpectedElements = 200_000_000, // = 1917011676 bits near 240 MB
        seed = Seed))
    println((df.take(5).map(_.userId).toSeq ++ Seq("Lorenzo Gardini", "Enrico Gallinucci"))
      .map(id => s"$id ${bloomFilter.contains(id)}")
      .mkString("\n"))

    val countMinSketchCoalescePartitionsNumber = 4 // 207 MB of bloom filter * 5 partitions = 1036 MB > max driver size
    val countMinSketch: CountMinSketch = decorateExecution("Create Count-min Sketch",
        createCountMinSketch(df.map(_.userId).coalesce(countMinSketchCoalescePartitionsNumber),
          errorProbability = 0.0001, // = 10 hash functions
          error = 0.000001, // = 2718282 counters for each function near 207 MB
          seed = Seed
        )
    )
    val approxPlacements = countMinSketch.getCount(mostActiveUserId)
    println(s"For user $mostActiveUserId, " +
      s"real count: $placements approximated: $approxPlacements\n" +
      s"error: ${(placements - approxPlacements)/placements * 100}%")
  }
}
