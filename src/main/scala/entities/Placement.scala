package entities

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try

trait Placement{
  val dateTime: Long
  val userId: String
  val pixelColor: String
}
case class TilePlacement(dateTime: Long, userId: String, pixelColor: String, coordinate: SingleCoordinate) extends Placement
case class AreaPlacement(dateTime: Long, userId: String, pixelColor: String, coordinate: RectangleCoordinate) extends Placement

object Placement {
  private val pattern = """([^,]+),([^,]+),([^,]+),"([^"]+)"""".r

  private val formatterWithThreeMillis = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS z")
  private val formatterWithOneMillis = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S z")
  private val formatterWithoutMillis = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z")

  private def parseDateTime(dateTimeString: String): Option[Long] = {
    Try(ZonedDateTime.parse(dateTimeString, formatterWithThreeMillis))
      .orElse(Try(ZonedDateTime.parse(dateTimeString, formatterWithOneMillis)))
      .orElse(Try(ZonedDateTime.parse(dateTimeString, formatterWithoutMillis)))
      .map(_.toInstant.toEpochMilli)
      .toOption
  }

  def apply(tilePlacement: String): Option[Placement] = tilePlacement match {
    case pattern(stringTimestamp, userId, color, stringCoordinate) =>
      val coordinatesParts = stringCoordinate.split(",").map(_.trim)
      parseDateTime(stringTimestamp) match {
        case Some(timestamp) =>
          Try(RectangleCoordinate(
            coordinatesParts(0).toInt,
            coordinatesParts(1).toInt,
            coordinatesParts(2).toInt,
            coordinatesParts(3).toInt
          )).toOption.map { coord => AreaPlacement(timestamp, userId, color.replace("#", ""), coord)
          }.orElse(
            Try(SingleCoordinate(coordinatesParts(0).toInt, coordinatesParts(1).toInt)).toOption.map { coord =>
              TilePlacement(timestamp, userId, color.replace("#", ""), coord)
            })
        case _ => None
      }
    case _ => None
  }
}
