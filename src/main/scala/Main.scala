import org.apache.spark.sql.SparkSession
import reader.InputReader
import transformation._
import java.io.File
import org.apache.commons.io.FileUtils

object Main {

  def cleanOutputDirs(paths: String*): Unit = {
    paths.foreach { path =>
      val dir = new File(path)
      if (dir.exists()) {
        println(s"Cleaning output directory")
        FileUtils.deleteDirectory(dir)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Flight Assignment")
      .master("local[*]")
      .getOrCreate()

    //  Clear output folder
    cleanOutputDirs("output")

    println("Starting Flight Assignment Processing...")

    try {
      val flights = InputReader.loadFlights(spark, "data/flightData.csv")
      val passengers = InputReader.loadPassengers(spark, "data/passengers.csv")

      if (!flights.isEmpty) {
        println("Running Question 1: Flights per month")
        Question1.run(flights, "output/q1_flights_per_month.csv")

        println("Running Question 2: Top 100 frequent flyers")
        Question2.run(flights, passengers, "output/q2_top_100_frequent_flyers.csv")

        println("Running Question 3: Longest non-UK journey")
        Question3.run(flights, "output/q3_longest_non_uk_run.csv")

        println("Running Question 4: Passengers who flew together more than 3 times")
        Question4.run(flights, "output/q4_flights_together.csv")

        println("Running Question 4 Extra: Flights together within a date range")
        Question4Extra.run(flights, 3, "2017-01-01", "2017-12-31", "output/q4_flights_together_by_range.csv")
      } else {
        println("No flight data found. Skipping processing.")
      }

    } finally {
      println("Stopping Spark session.")
      spark.stop()
    }

    println("Flight Assignment Processing Complete.")
  }
}
