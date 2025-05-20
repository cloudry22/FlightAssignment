package transformation

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import model.{FlightData, FlightsPerMonth}

object Question1 {
  def run(flightDS: Dataset[FlightData], outputPath: String): Unit = {
    import flightDS.sparkSession.implicits._

    val result = flightDS
      .withColumn("month", month(to_date($"date", "yyyy-MM-dd")))
      .select("flightId", "month").distinct()
      .groupBy("month").count()
      .as[(Int, Long)]
      .map { case (month, count) => FlightsPerMonth(month, count) }
      .orderBy("month")

    result
      .show()

    result
      .toDF()
      .withColumnRenamed("month", "Month")
      .withColumnRenamed("numberOfFlights", "Number of Flights")
      .coalesce(1)
      .write.option("header", "true").csv(outputPath)
  }
}