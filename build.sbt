name := "UntiNots_technical_test"

version := "0.1"

scalaVersion := "2.11.11"

val spark_Version = "2.2.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % spark_Version
libraryDependencies += "org.apache.spark" %%  "spark-sql-kafka-0-10" % spark_Version
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % spark_Version
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.0"

libraryDependencies += "log4j" % "log4j" % "1.2.17"


