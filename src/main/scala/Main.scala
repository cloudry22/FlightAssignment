import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Case classes for structured and typed datasets
case class FlightData(passengerId: Int, flightId: Int, from: String, to: String, date: String)
case class Passenger(passengerId: Int, firstName: String, lastName: String)
case class FlightsPerMonth(month: Int, numberOfFlights: Long)
case class FrequentFlyer(passengerId: Int, numberOfFlights: Long, firstName: String, lastName: String)
case class LongestNonUKRun(passengerId: Int, longestRun: Int)
case class FlightsTogether(passenger1Id: Int, passenger2Id: Int, numberOfFlightsTogether: Long)

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Flight Assignment")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Define input schemas
    val flightSchema = StructType(Seq(
      StructField("passengerId", IntegerType, nullable = true),
      StructField("flightId", IntegerType, nullable = true),
      StructField("from", StringType, nullable = true),
      StructField("to", StringType, nullable = true),
      StructField("date", StringType, nullable = true)
    ))

    val passengerSchema = StructType(Seq(
      StructField("passengerId", IntegerType, nullable = true),
      StructField("firstName", StringType, nullable = true),
      StructField("lastName", StringType, nullable = true)
    ))

    val flightInputPath = "data/flightData.csv"
    val passengerInputPath = "data/passengers.csv"

    val rawFlightDF = spark.read
      .option("header", "true")
      .schema(flightSchema)
      .csv(flightInputPath)

    val flightDataset = rawFlightDF
      .filter(row => isValidFlight(row))
      .as[FlightData]

    val rawPassengerDF = spark.read
      .option("header", "true")
      .schema(passengerSchema)
      .csv(passengerInputPath)

    val passengerDataset = rawPassengerDF
      .filter(row => isValidPassenger(row))
      .as[Passenger]

    // Execute all questions
    println("Running Q1: Flights per month...")
    if (!flightDataset.isEmpty) runQuestion1(flightDataset)

    println("Running Q2: Top 100 frequent flyers...")
    if (!flightDataset.isEmpty && !passengerDataset.isEmpty) runQuestion2(flightDataset, passengerDataset)

    println("Running Q3: Longest run without being in UK...")
    if (!flightDataset.isEmpty) runQuestion3(flightDataset)

    println("Running Q4: Flights together more than 3 times...")
    if (!flightDataset.isEmpty) runQuestion4(flightDataset)

    println("Running Extra Q4: Flights together within date range...")
    if (!flightDataset.isEmpty) runQuestion4Extra(flightDataset, 3, "2017-01-01", "2017-12-31")

    spark.stop()
  }

  /** Helper to validate that date column is not null or empty */
   def isValidDate(row: Row): Boolean = {
    val date = row.getAs[String]("date")
    date != null && date.nonEmpty
  }

  /** Helper to validate a flight row before mapping to FlightData */
   def isValidFlight(row: Row): Boolean = {
    row.getAs[Integer]("passengerId") != null &&
      row.getAs[Integer]("flightId") != null &&
      isValidDate(row)
  }

  /** Helper to validate a passenger row before mapping to Passenger */
   def isValidPassenger(row: Row): Boolean = {
    row.getAs[Integer]("passengerId") != null &&
      row.getAs[String]("firstName") != null &&
      row.getAs[String]("lastName") != null
  }

  /**
   * Helper for Extra Q4: Find passengers who have been on more than N flights together within a date range.
   *
   * @param flightDS Dataset containing flight data
   * @param atLeastNTimes Minimum number of flights passengers must have taken together
   * @param from Start date (inclusive) in "yyyy-MM-dd"
   * @param to End date (inclusive) in "yyyy-MM-dd"
   * @return Dataset[FlightsTogether]
   */
   def flownTogether(flightDS: Dataset[FlightData], atLeastNTimes: Int, from: String, to: String): Dataset[FlightsTogether] = {
    import flightDS.sparkSession.implicits._

    val filteredFlights = flightDS
      .filter(f => f.date != null && f.date >= from && f.date <= to)
      .select($"flightId", $"passengerId")
      .as[(Int, Int)]

    val joinedPairs = filteredFlights
      .toDF("flightId", "passenger1")
      .join(filteredFlights.toDF("flightId", "passenger2"), Seq("flightId"))
      .filter($"passenger1" < $"passenger2")

    joinedPairs
      .groupBy("passenger1", "passenger2")
      .count()
      .filter($"count" > atLeastNTimes)
      .map(row => FlightsTogether(
        row.getAs[Int]("passenger1"),
        row.getAs[Int]("passenger2"),
        row.getAs[Long]("count")
      ))
  }

  /** Q1: Compute total flights per month */
  def runQuestion1(flightDS: Dataset[FlightData]): Unit = {
    import flightDS.sparkSession.implicits._
    val outputPath = "output/q1_flights_per_month.csv"

    // Compute total flights per month using case class
    val result = flightDS
      .withColumn("month", month(to_date($"date", "yyyy-MM-dd")))
      .groupBy("month")
      .count()
      .as[(Int, Long)]
      .map { case (month, count) => FlightsPerMonth(month, count) }
      .orderBy("month")

    result.show()

  try {
      result.toDF()
        .withColumnRenamed("month", "Month")
        .withColumnRenamed("numberOfFlights", "Number of Flights")
        .coalesce(1)
        .write
        .option("header", "true")
        .csv(outputPath)
    } catch {
      case e: Exception => println(s"[ERROR] Could not write Q1 results: ${e.getMessage}")
    }
  }

  /** Q2: Top 100 most frequent flyers */
  def runQuestion2(flightDS: Dataset[FlightData], passengerDS: Dataset[Passenger]): Unit = {
    import flightDS.sparkSession.implicits._
    val outputPath = "output/q2_top_100_frequent_flyers.csv"

    val result = flightDS
      .groupBy("passengerId")
      .count()
      .withColumnRenamed("count", "numberOfFlights")
      .join(passengerDS, "passengerId")
      .select($"passengerId", $"numberOfFlights", $"firstName", $"lastName")
      .as[FrequentFlyer]
      .orderBy(desc("numberOfFlights"))
      .limit(100)
      .toDF()
      .withColumnRenamed("passengerId", "Passenger ID")
      .withColumnRenamed("numberOfFlights", "Number of Flights")
      .withColumnRenamed("firstName", "First name")
      .withColumnRenamed("lastName", "Last name")

    result.show()

    try {
      result.coalesce(1)
        .write.option("header", "true")
        .csv(outputPath)
    } catch {
      case e: Exception => println(s"[ERROR] Could not write Q2 results: ${e.getMessage}")
    }
  }

  /** Q3: Longest run without being in UK */
   def runQuestion3(flightDS: Dataset[FlightData]): Unit = {
    import flightDS.sparkSession.implicits._
    val outputPath = "output/q3_longest_non_uk_run.csv"

    val result = flightDS
      .groupByKey(_.passengerId)
      .mapGroups { case (pid, flights) =>
        val sorted = flights.toSeq.sortBy(_.date)
        LongestNonUKRun(pid, computeLongestRunWithoutUK(sorted))
      }
      .orderBy(desc("longestRun"))
      .toDF()
      .withColumnRenamed("passengerId", "Passenger ID")
      .withColumnRenamed("longestRun", "Longest Run")

    result.show()

    try {
      result
        .coalesce(1)
        .write.option("header", "true")
        .csv(outputPath)
    } catch {
      case e: Exception => println(s"[ERROR] Could not write Q3 results: ${e.getMessage}")
    }
  }

  // Computes the longest continuous run of flights not involving the UK
  def computeLongestRunWithoutUK(flights: Seq[FlightData]): Int = {
    var maxRun = 0
    var currentRun = 0
    for (flight <- flights) {
      if (flight.from != "UK" && flight.to != "UK") {
        currentRun += 1
        maxRun = math.max(maxRun, currentRun)
      } else {
        currentRun = 0
      }
    }
    maxRun
  }

  /** Q4: Find passengers who flew together more than 3 times */
  def runQuestion4(flightDS: Dataset[FlightData]): Unit = {
    import flightDS.sparkSession.implicits._
    val outputPath = "output/q4_flights_together.csv"

    val flightPairs = flightDS
      .select($"flightId", $"passengerId")
      .as[(Int, Int)]
      .cache()

    // Rename flightPairs for self-join
    val a = flightPairs.toDF("flightId", "p1")
    val b = flightPairs.toDF("flightId", "p2")

    val joined = a.join(b, Seq("flightId"))
      .filter($"p1" < $"p2") // avoid self-pairs and duplicates
      .select($"p1", $"p2")
      .as[(Int, Int)]

    // Count flights together
    val result = joined
      .groupByKey(identity)
      .count()
      .filter(_._2 > 3)
      .map { case ((p1, p2), count) =>
        FlightsTogether(p1, p2, count)
      }
      .orderBy(desc("numberOfFlightsTogether"))

    result.show()

    try {
      result
        .map(r => (r.passenger1Id, r.passenger2Id, r.numberOfFlightsTogether))
        .toDF("Passenger 1 ID", "Passenger 2 ID", "Number of Flights Together")
        .coalesce(1)
        .write
        .option("header", "true")
        .csv(outputPath)
    } catch {
      case e: Exception => println(s"[ERROR] Could not write Q4 results: ${e.getMessage}")
    }
  }

  /**
   * Extra Q4: Save passengers who flew together more than N times within a date range.
   */
  def runQuestion4Extra(flightDS: Dataset[FlightData], atLeastNTimes: Int, from: String, to: String): Unit = {
    import flightDS.sparkSession.implicits._
    val outputPath = "output/q4_flights_together_by_range.csv"

    val result = flownTogether(flightDS, atLeastNTimes, from, to)

    try {
      result
        .orderBy(desc("numberOfFlightsTogether"))
        .map(r => (r.passenger1Id, r.passenger2Id, r.numberOfFlightsTogether, from, to))
        .toDF("Passenger 1 ID", "Passenger 2 ID", "Number of Flights Together", "From", "To")
        .coalesce(1)
        .write
        .option("header", "true")
        .csv(outputPath)
    } catch {
      case e: Exception => println(s"[ERROR] Could not write Extra Q4 results: ${e.getMessage}")
    }
  }


}


