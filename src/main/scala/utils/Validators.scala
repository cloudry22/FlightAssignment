package utils

import org.apache.spark.sql.Row

object Validators {

  /** Function to validate a flight row before mapping to FlightData */
  val isValidFlight: Row => Boolean = row =>
    Option(row.getAs[Any]("passengerId")).isDefined &&
      Option(row.getAs[Any]("flightId")).isDefined &&
      Option(row.getAs[String]("date")).exists(_.nonEmpty)

  /** Function to validate a passenger row before mapping to Passenger */
  val isValidPassenger: Row => Boolean = row =>
    Option(row.getAs[Any]("passengerId")).isDefined &&
      Option(row.getAs[String]("firstName")).exists(_.nonEmpty) &&
      Option(row.getAs[String]("lastName")).exists(_.nonEmpty)
}
