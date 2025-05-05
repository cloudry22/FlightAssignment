name := "FlightAssignment"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql"  % "2.4.8",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)
