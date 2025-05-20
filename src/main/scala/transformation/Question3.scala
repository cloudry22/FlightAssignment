package transformation

import org.apache.spark.sql.Dataset
import model.{FlightData, LongestNonUKRun}

object Question3 {
  def computeLongestRunWithoutUK(flights: Seq[FlightData]): Int = {
    var longest = 0
    var currentCountries = scala.collection.mutable.LinkedHashSet[String]()
    val sortedFlights = flights.sortBy(_.date)

    for (flight <- sortedFlights) {
      val from = Option(flight.from).map(_.trim.toUpperCase).getOrElse("")
      val to = Option(flight.to).map(_.trim.toUpperCase).getOrElse("")

      if (from == "UK" || to == "UK") {
        longest = math.max(longest, currentCountries.size)
        currentCountries.clear()
      } else {
        currentCountries += from
        currentCountries += to
      }
    }

    math.max(longest, currentCountries.size)
  }

  def run(flightDS: Dataset[FlightData], outputPath: String): Unit = {
    import flightDS.sparkSession.implicits._

    val result = flightDS
      .groupByKey(_.passengerId)
      .mapGroups { case (pid, flights) =>
        LongestNonUKRun(pid, computeLongestRunWithoutUK(flights.toSeq))
      }
      .orderBy($"longestRun".desc)

    result
      .show()

    result
      .toDF()
      .withColumnRenamed("passengerId", "Passenger ID")
      .withColumnRenamed("longestRun", "Longest Run")
      .coalesce(1)
      .write.option("header", "true").csv(outputPath)
  }
}