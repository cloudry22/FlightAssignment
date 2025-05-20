package transformation

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import model.{FlightData, Passenger, FrequentFlyer}

object Question2 {
  def run(flightDS: Dataset[FlightData], passengerDS: Dataset[Passenger], outputPath: String): Unit = {
    import flightDS.sparkSession.implicits._

    val result = flightDS
      .groupBy("passengerId").count()
      .withColumnRenamed("count", "numberOfFlights")
      .join(passengerDS, "passengerId")
      .select($"passengerId", $"numberOfFlights", $"firstName", $"lastName")
      .as[FrequentFlyer]
      .orderBy(desc("numberOfFlights"))
      .limit(100)

    result
      .show()

    result
      .toDF()
      .withColumnRenamed("passengerId", "Passenger ID")
      .withColumnRenamed("numberOfFlights", "Number of Flights")
      .withColumnRenamed("firstName", "First name")
      .withColumnRenamed("lastName", "Last name")
      .coalesce(1)
      .write.option("header", "true").csv(outputPath)
  }
}