package entities

trait Coordinate
case class SingleCoordinate(x: Int, y: Int) extends Coordinate
case class RectangleCoordinate(x1: Int, y1: Int, x2: Int, y2: Int) extends Coordinate

