package scripts

import java.util.Properties

import org.apache.log4j.{Level, Logger}

import collection.mutable
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}

/**
 *
 */
object ConsumerData extends App {

  readFromKafka("Queue1")

  /**
   *
   * @param topic
   */
  def readFromKafka(topic: String): Unit = {
  Logger.getLogger("org").setLevel(Level.ERROR)
    // Create context Spark
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("UntieNots")

    // configuration Kafka properties
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // list of topics keywords
    val topicsKeywords = new mutable.HashMap[String, List[String]]

    // add a list of Keywords with their words
    def addKeywords(key: String, value: String) = {
      topicsKeywords += (key -> (value :: (topicsKeywords get key getOrElse Nil)))
    }
    addKeywords("Name", "ADAM")
    addKeywords("Name", "Alice")
    addKeywords("Name", "CELIA")
    addKeywords("Name", "OLIVER")
    addKeywords("Name", "Pamela")
    addKeywords("Name", "Judith")
    addKeywords("Name", "Paul")
    addKeywords("Name", "Gutenberg")

    addKeywords("Job", "Journalist")

    // set kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "consumer-group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean) //If true the consumer's offset will be periodically committed in the background.
    )
    // main entry point for Spark Streaming functionnality
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val topics = Array(topic)
    val stream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent, // distribute partitions evenly accross available executors
      Subscribe[String, String](topics, kafkaParams) // susbscribe to Queue1 topic
    )
    // iterate for each rdd
    stream.foreachRDD( rdd => {
      rdd.foreach( line => {
        val sourceStream = rdd.map(x => x.key())
        val wordStream = rdd.map(x => x.value())
      //  val mapKV = rdd.map(x => (x.key(),x.value()))
      //  val cmap = mapKV.count()
        //   mapKV.foreach(println)


        val recordQueue2: ProducerRecord[String,String] = new ProducerRecord("Queue2", sourceStream, "test")
    producer.send(recordQueue2)

      }
      )
      // iterate in keyword List
   /*  for ((topic,keyword) <- topicsKeywords) {
        // if the word is in the keyword list send in Queue2
        //Q2: {"source": <file_name >, "word": <word>, "topics": [<topics>] }
        //TODO Array topics
      /* val msg = String.format("{\"numbers\": %s, \"timestamp\": \"%s\"}",
         java.util.Arrays.toString(sourceStream), wordStream);*/

        if (wordStream == keyword) {
          val recordQueue2: =  new ProducerRecord("Queue2", "sourceStream", "wordStream")
          producer.send(recordQueue2)
        }

         //If the word corresponds to a topic name, send in Queue3
        // Q3: {"source": <file_name>,"topic": <topic>}
        if (wordStream == topic) {
          val recordQueue3 = new ProducerRecord("Queue3", "test", "wordStream")
          producer.send(recordQueue3)
        }
      }*/
    })

ssc.start()
ssc.awaitTermination()
}
}
