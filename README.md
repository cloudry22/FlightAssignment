
# ✈️ Flight Assignment - Spark Scala Project

This project processes flight and passenger data using Apache Spark with Scala. It computes several analytics such as monthly flight counts, frequent flyers, longest non-UK sequences, and co-flying passengers.

---

## 📂 Project Structure

```
FlightAssignment/
├── build.sbt
├── project/
├── src/
│   ├── main/
│   │   └── scala/
│   │       └── Main.scala
│   └── test/
│       └── scala/
│           └── MainSpec.scala
├── data/
│   ├── flightData.csv
│   └── passengers.csv
├── output/ (auto-generated)
├── .gitignore
└── README.md
```

---

## 🧠 Functionalities

### Main Analysis (`Main.scala`)
1. **Q1:** Total flights per month
2. **Q2:** Top 100 most frequent flyers
3. **Q3:** Longest sequence of flights not involving UK
4. **Q4:** Pairs of passengers who flew together >3 times
5. **Q4 Extra:** Same as Q4, but filtered within a custom date range (e.g., 2017-01-01 to 2017-12-31)

Each output is saved as a CSV file inside the `/output` folder.

---

## ✅ How to Run

### Prerequisites
- Java 8 (JDK)
- Scala (2.12.10)
- sbt (1.10+)
- Spark (2.4.8)

### Running the program

From the project root:

```bash
sbt run
```

This will:
- Load the data from `data/`
- Process the analytics
- Save results to the `output/` folder

---

## 🧪 Running Unit Tests

Tests are located in `src/test/scala/MainSpec.scala`.

To run all unit tests:

```bash
sbt test
```

Example tested function:
- `computeLongestRunWithoutUK()`: Verifies correct computation of longest non-UK flight sequences.

---

## 🛠 Technologies Used

- Scala 2.12.10
- Apache Spark 2.4.8
- sbt for build and dependency management
- ScalaTest for unit testing

---

## 📁 Output Samples

- `q1_flights_per_month.csv`
- `q2_top_100_frequent_flyers.csv`
- `q3_longest_non_uk_run.csv`
- `q4_flights_together.csv`
- `q4_flights_together_by_range.csv` (example of answer)

---

## 🧼 Notes

- `.gitignore` is configured to exclude IntelliJ files, target directories, and IDE cache folders.
- This project is structured for clarity, performance, and testability.

---

## 👨‍💻 Author

Helmi Asari – [Data Engineer] – Malaysia  
