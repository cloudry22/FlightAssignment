package transformation

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import model.{FlightData, FlightsTogether}

object Question4 {
  def run(flightDS: Dataset[FlightData], outputPath: String): Unit = {
    import flightDS.sparkSession.implicits._

    val pairs = flightDS.select("flightId", "passengerId").as[(Int, Int)].cache()

    val joined = pairs.toDF("flightId", "p1")
      .join(pairs.toDF("flightId", "p2"), Seq("flightId"))
      .filter($"p1" < $"p2")
      .select("p1", "p2").as[(Int, Int)]

    val result = joined.groupByKey(identity).count()
      .filter(_._2 > 3)
      .map { case ((p1, p2), count) => FlightsTogether(p1, p2, count) }
      .orderBy(desc("numberOfFlightsTogether"))

    result
      .show()

    result
      .map(r => (r.passenger1Id, r.passenger2Id, r.numberOfFlightsTogether))
      .toDF("Passenger 1 ID", "Passenger 2 ID", "Number of Flights Together")
      .coalesce(1)
      .write.option("header", "true").csv(outputPath)
  }
}
