package transformation

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import model.{FlightData, FlightsTogether}

object Question4Extra {
  def run(flightDS: Dataset[FlightData], atLeastNTimes: Int, from: String, to: String, outputPath: String): Unit = {
    import flightDS.sparkSession.implicits._

    val filtered = flightDS.filter(f => f.date != null && f.date >= from && f.date <= to)
      .select("flightId", "passengerId").as[(Int, Int)]

    val joined = filtered.toDF("flightId", "p1")
      .join(filtered.toDF("flightId", "p2"), Seq("flightId"))
      .filter($"p1" < $"p2")

    val result = joined.groupBy("p1", "p2").count()
      .filter($"count" > atLeastNTimes)
      .map(row => FlightsTogether(
        row.getAs[Int]("p1"),
        row.getAs[Int]("p2"),
        row.getAs[Long]("count")
      ))

    result
      .show()

    result.orderBy(desc("numberOfFlightsTogether"))
      .map(r => (r.passenger1Id, r.passenger2Id, r.numberOfFlightsTogether, from, to))
      .toDF("Passenger 1 ID", "Passenger 2 ID", "Number of Flights Together", "From", "To")
      .coalesce(1)
      .write.option("header", "true").csv(outputPath)
  }
}
