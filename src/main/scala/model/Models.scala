package model

case class FlightData(passengerId: Int, flightId: Int, from: String, to: String, date: String)
case class Passenger(passengerId: Int, firstName: String, lastName: String)
case class FlightsPerMonth(month: Int, numberOfFlights: Long)
case class FrequentFlyer(passengerId: Int, numberOfFlights: Long, firstName: String, lastName: String)
case class LongestNonUKRun(passengerId: Int, longestRun: Int)
case class FlightsTogether(passenger1Id: Int, passenger2Id: Int, numberOfFlightsTogether: Long)