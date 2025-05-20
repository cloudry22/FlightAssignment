package reader

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._
import model.{FlightData, Passenger}
import utils.Validators

object InputReader {
  def loadFlights(spark: SparkSession, path: String): Dataset[FlightData] = {
    val schema = StructType(Seq(
      StructField("passengerId", IntegerType),
      StructField("flightId", IntegerType),
      StructField("from", StringType),
      StructField("to", StringType),
      StructField("date", StringType)
    ))
    import spark.implicits._
    spark.read.option("header", "true").schema(schema).csv(path)
      .filter(Validators.isValidFlight).as[FlightData]
  }

  def loadPassengers(spark: SparkSession, path: String): Dataset[Passenger] = {
    val schema = StructType(Seq(
      StructField("passengerId", IntegerType),
      StructField("firstName", StringType),
      StructField("lastName", StringType)
    ))
    import spark.implicits._
    spark.read.option("header", "true").schema(schema).csv(path)
      .filter(Validators.isValidPassenger).as[Passenger]
  }
}
