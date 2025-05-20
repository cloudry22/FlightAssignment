# ✈️ Flight Assignment – Spark Scala Project

This project processes flight and passenger data using **Apache Spark** in **Scala**. It computes several insights including flight volumes, frequent flyers, unique travel sequences, and co-travel patterns.

## 📁 Project Structure

```
FlightAssignment/
├── build.sbt
├── project/
├── src/
│   └── main/
│       └── scala/
│           ├── Main.scala
│           ├── model/
│           │   └── [case classes]
│           ├── reader/
│           │   └── InputReader.scala
│           ├── transformation/
│           │   ├── Question1.scala
│           │   ├── Question2.scala
│           │   ├── Question3.scala
│           │   ├── Question4.scala
│           │   └── Question4Extra.scala
│           └── utils/
│               └── Validators.scala
│
├── src/
│   └── test/
│       └── scala/
│           └── MainSpec.scala
│
├── data/
│   ├── flightData.csv
│   └── passengers.csv
├── output/               # auto-generated result files
├── .gitignore
└── README.md
```

## 🧠 Functionalities

The `Main.scala` script orchestrates the execution of the following modules:

### ✅ Q1: Flights Per Month
- Counts **unique flights per month**.
- Ensures flights aren't double-counted if multiple passengers were on the same flight.

### ✅ Q2: Top 100 Frequent Flyers
- Calculates the top 100 passengers with the most flights.
- Enriches results with passenger names.

### ✅ Q3: Longest Run Without Entering UK
- Identifies the **longest continuous sequence** of countries a passenger traveled through without being in the **UK**.
- UK interruptions reset the count.
- Only unique countries between UKs are counted.

### ✅ Q4: Passengers Who Flew Together
- Identifies pairs of passengers who were on the same flight **more than 3 times**.

### ✅ Q4 Extra: Flights Together Within Date Range
- Same logic as Q4 but filters by a given **date range**.

## ✅ How to Run

### Prerequisites

Ensure the following are installed:

- **Java 8 (JDK)**
- **Scala 2.12.10**
- **Apache Spark 2.4.8**
- **sbt 1.10+**

### Run the Program

From the project root directory, execute:

```bash
sbt run
```

This will:

- Load the flight and passenger data from `/data`
- Clear previous results in `/output`
- Generate 5 CSV outputs for each question in the `/output` directory

## 🧪 Running Unit Tests

Tests are located in:

```
src/test/scala/MainSpec.scala
```

To execute them:

```bash
sbt test
```

Currently tested:

- `computeLongestRunWithoutUK`: Ensures accurate country count ignoring UK appearances

## 📂 Output Samples

- `output/q1_flights_per_month.csv`
- `output/q2_top_100_frequent_flyers.csv`
- `output/q3_longest_non_uk_run.csv`
- `output/q4_flights_together.csv`
- `output/q4_flights_together_by_range.csv`

## 🧼 Notes

- `.gitignore` is configured to exclude IntelliJ metadata, `/target` folders, and `.idea/` cache
- Project is modularized for **clarity**, **reusability**, and **testability**
- Code follows good separation of concerns with utilities, models, and transformations in their own packages

## 👨‍💻 Author

**Helmi Asari** – Data Engineer – Malaysia