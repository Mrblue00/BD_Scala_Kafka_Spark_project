package scripts

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataAnalysis extends  App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  // spark configuration
  // TODO do DataAnalysis
  val spark = SparkSession.builder()
    .appName("SavingQueues")
    .master("local[*]")
    .getOrCreate()

  val dfQueue2 = spark.read.parquet("./data/outputQueues2/")

  val dfQueue3 = spark.read.parquet("./data/outputQueues3/")

}
