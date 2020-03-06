package scripts

import java.io.File
import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import scala.io.Source

object ProducerDataBatch extends App {
  sendToKafka("Queue1")

  def sendToKafka(topic: String): Unit = {

    // configuration Kafka properties
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // get files from directory "data"
    val pathDirectory = "./data/filesProducer"
    val files = getListFile(pathDirectory)

    // For each file we split text in words and send them in Producer queue
    files.foreach(file => {
      val fileName = file.getName()
      val fileContents = Source.fromFile(file).getLines
      val words = fileContents.flatMap(x => x.split("\\W+"))

      // for each word we send in the Queue as the value and the FileName as the key
      words.foreach(word => {
        // setting Producer records
        val record = new ProducerRecord(topic, fileName, word)
        producer.send(record)
      })
    })
    producer.close()
  }

  // get List File of the directory "data"
  def getListFile(dir: String): List[File] = {
    val d = new File(dir)

    // check if directories exist
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
}

