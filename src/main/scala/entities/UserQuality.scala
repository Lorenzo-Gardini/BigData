package entities

trait UserQuality

case object HighlyActiveUser extends UserQuality
case object ModeratelyActiveUser extends UserQuality
case object LowActivityUser extends UserQuality
