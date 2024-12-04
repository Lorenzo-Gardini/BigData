package entities

sealed trait SpotQuality

case object BadSpot extends SpotQuality
case object GoodSpot extends SpotQuality
case object AverageSpot extends SpotQuality