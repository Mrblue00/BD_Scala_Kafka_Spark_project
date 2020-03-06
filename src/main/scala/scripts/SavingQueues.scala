package scripts

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SavingQueues extends App {

  // TODO finalize schema file parquet
 // import spark.implicits._

  // schema for parquet file Queue2
  /*val queue2Schema = StructType(Array(
    StructField("source", StringType, nullable = true),
    StructField("word", StringType, nullable = true),
    StructField("topics", StringType, nullable = true)
  ))*/

  // schema for parquet file Queue3
 /* val queue3Schema = StructType(Array(
    StructField("source", StringType, nullable = true),
    StructField("topic", StringType, nullable = true)
  ))*/

  Logger.getLogger("org").setLevel(Level.ERROR)
  // spark configuration
  val spark = SparkSession.builder()
    .appName("SavingQueues")
    .master("local[*]")
    .getOrCreate()

  // Subscribe to topic Queue2 - creating DataFrame
  val Queue2DF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "Queue2")
    .option("startingOffsets", "latest") // reading from lastest offset using for streaming
    .load()

  //val df1 = Queue2DF.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
  // writing and saving stream into  Queue2.parquet

  val QueryQueue2 = Queue2DF.writeStream
    .outputMode("append") // OutputMode in which only the new rows in the streaming DF will be written to the sink.
    .format("parquet")
    .option("checkpointLocation", "checkpoint/QueryQueue2") // set checkpoint location for retrieve datas
    .start("./data/output/outputQueues2")

  // Subscribe to topic Queue3 - creating DataFrame
  val columnsQueue3 = Seq("source", "topic")
  val Queue3DF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "Queue3")
    .option("startingOffsets", "latest")
    .load()

  // writing and saving stream into  Queue3.parquet
  val QueryQueue3 = Queue3DF.writeStream
    .outputMode("append")
    .format("parquet")
    .option("checkpointLocation", "checkpoint/QueryQueue3")
    .start("./data/output/outputQueues3")

  spark.streams.awaitAnyTermination() // handle multiple Streams in parallel
}

