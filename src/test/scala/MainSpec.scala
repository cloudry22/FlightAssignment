import model.FlightData
import org.scalatest.FunSuite
import transformation.Question3.computeLongestRunWithoutUK

class MainSpec extends FunSuite {

  test("computeLongestRunWithoutUK should return correct count with UK in between") {
    val flights = Seq(
      FlightData(1, 101, "DE", "FR", "2017-01-01"), // Non-UK
      FlightData(1, 102, "FR", "ES", "2017-01-02"), // Non-UK
      FlightData(1, 103, "ES", "UK", "2017-01-02"), // UK appears, resets run
      FlightData(1, 104, "UK", "DE", "2017-01-03"), // UK appears, resets run
      FlightData(1, 105, "DE", "NL", "2017-01-04"), // Non-UK
    )

    val result = computeLongestRunWithoutUK(flights)
    assert(result == 3) // DE, FR, ES
  }

  test("computeLongestRunWithoutUK should return 0 if all flights involve UK") {
    val flights = Seq(
      FlightData(2, 201, "UK", "FR", "2017-02-01"),
      FlightData(2, 202, "FR", "UK", "2017-02-02")
    )

    val result = computeLongestRunWithoutUK(flights)
    assert(result == 0)
  }

  test("computeLongestRunWithoutUK should return full length if UK not involved at all") {
    val flights = Seq(
      FlightData(3, 301, "DE", "FR", "2017-03-01"),
      FlightData(3, 302, "FR", "ES", "2017-03-02"),
      FlightData(3, 303, "ES", "NL", "2017-03-03")
    )

    val result = computeLongestRunWithoutUK(flights)
    assert(result == 4) // DE, FR, ES, NL
  }

  test("computeLongestRunWithoutUK should handle empty list") {
    val result = computeLongestRunWithoutUK(Seq.empty)
    assert(result == 0)
  }
}
