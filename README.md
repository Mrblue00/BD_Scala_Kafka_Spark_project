
# UntieNots_technical_test

Technical Projet using big Datas technologies such as Spark/ Spark Streaming/ Kafka.

For using this project you need to have a kafka instance to launch Kafka Server and Zookeeper server.
	
We need to create Three topics for each Queue :

	• kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic Queue1
	• kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic Queue2
	• kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic Queue3

This projet focus on the technologies of Big Data and most particulary the technologies of Kafka, Spark, Spark Streaming.

##objectives

I need to write 4 Scripts with the objectives below :

1 - First script Produce data in a batch

	a.Open a directory containing text files from a corpus of your choice.

	b.Split text files in words.
	
	c.Send them in a kafka queue (Q1) : {"source": <file_name >, "word": <word>}
	
2 - Second script: Read data from queue Q1 (streaming)

	a.Load a list of topics that should be monitored (a topic has a name and a list of keywords, ex
	{"color": ['red', 'blue', 'green']}, {"sport": ['football', 'tennis', 'horseriding']} {"plane": ['wing',
	'pilot', 'propeller']})
	
	b.Read queue Q1 with spark streaming

	c.If the word is in the keyword list for one or several topics, send a message in another queue
	Q2: {"source": <file_name >, "word": <word>, "topics": [<topics>] }
	
	d.If the word corresponds to a topic name, send in a third queue Q3: {"source": <file_name>,
	"topic": <topic>}
	
3 - Third script: Saving of queues Q2 and Q3 (batch)

	a.Read queue Q2 since last offset and save its content in .parquet files

	b.Read queue Q3 since last offset and save its content in .parquet files
	
4 - Fourth script : Data analysis

	a.Open the exported parquet files.

	b.For each topic find :
		- the sources associated with the number of occurrences for each key word.
		- the false positives (sources identified with the keywords that do not belong to the topic) =>
	We assume that a source belongs to a topic if X% of its keywords can be found in the source.
	(X is an argument of the script).

	c.the relevance of each keyword: rate of presence in a source belonging to the topic/ rate of
	presence in a topic not belonging to the topic / rate of absence in a topic that belonged to
	the topic

##Environnement

For Processing datas i'am using a corpus of text files findable at src : http://corpus.canterbury.ac.nz/descriptions/#cantrbry

	1. alice28.txt -> size : 152089 bytes
	2. asyoulik.txt -> size : 125179 bytes
	3. lcet10.txt -> size : 426754 bytes
	4. plrabn12.txt -> size : 481861 bytes

For testing this project you need to have a kafka instance localy to launch Kafka Server and Zookeeper server.

1
Create Three topic with Kafka for each Queue :

	• kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic Queue1
	• kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic Queue2
	• kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic Queue3

I create 4 files each Scripts wanted :

	1.ProducerDataBatch.scala
	2.ConsumerData.scala
	3.SavingQueues.scala
	4.DataAnalysis.scala
	
Run one per one these scripts.

I'am using IntelliJ Community as IDE and sbt for managing my dependencies.

##Explaination

I have tested each step with Producer and/or Consummer on the Kafka console for checking if datas were correctly process throught Kafka.

For the ProducerDataBatch.scala i used :

	• kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic Queue1 --from-beginning*
	
it returns me the source of the files process and the word associated with it ("source","word")

For the second script ConsumerData.scala i used :

	• kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic Queue3 --from-beginning
	• kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic Queue2 --from-beginning
	
##Difficulty

I spend a lot of time on looking at documentation of Kafka and Spark.
I lost half a day on configuration errors.

Error : org.apache.spark.SparkException: Task not serializable


ProducerDataBatch.scala : delete blank word

ConsumerData.scala : solve the exception not serializable

##Sources used

•https://blog.engineering.publicissapient.fr/2017/09/27/spark-comprendre-et-corriger-lexception-task-not-serializable/
•https://kafka.apache.org/documentation/
•https://spark.apache.org/docs/latest/streaming-programming-guide.html

