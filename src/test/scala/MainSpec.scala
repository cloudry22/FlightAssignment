import Main.computeLongestRunWithoutUK
import org.scalatest.FunSuite

class MainSpec extends FunSuite {

  test("computeLongestRunWithoutUK should return correct count") {
    val flights = Seq(
      FlightData(1, 101, "DE", "FR", "2017-01-01"), // Non-UK
      FlightData(1, 102, "FR", "ES", "2017-01-02"), // Non-UK
      FlightData(1, 103, "UK", "DE", "2017-01-03"), // Ends run
      FlightData(1, 104, "DE", "NL", "2017-01-04"), // Non-UK
      FlightData(1, 105, "NL", "PL", "2017-01-05")  // Non-UK
    )

    val result = computeLongestRunWithoutUK(flights)
    assert(result == 2)
  }

  test("computeLongestRunWithoutUK returns 0 when all flights involve UK") {
    val flights = Seq(
      FlightData(2, 201, "UK", "FR", "2017-02-01"),
      FlightData(2, 202, "FR", "UK", "2017-02-02")
    )

    assert(computeLongestRunWithoutUK(flights) == 0)
  }

  test("computeLongestRunWithoutUK returns full length when no UK flights") {
    val flights = Seq(
      FlightData(3, 301, "DE", "FR", "2017-03-01"),
      FlightData(3, 302, "FR", "ES", "2017-03-02"),
      FlightData(3, 303, "ES", "NL", "2017-03-03")
    )

    assert(computeLongestRunWithoutUK(flights) == 3)
  }
}
