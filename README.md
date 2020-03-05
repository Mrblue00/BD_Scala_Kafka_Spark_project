# UntieNots_technical_test
Technical Projet using Kafka / Spark Streaming

For using this project you need to have a kafka instance to launch Kafka Server and Zookeeper server.

	• Start zookeeper

	• Start kafka with default configuration
	
	• Create Three topic for each Queue :
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic Queue1
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic Queue2
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic Queue3

