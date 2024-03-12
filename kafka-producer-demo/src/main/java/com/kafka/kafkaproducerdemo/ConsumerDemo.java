package com.kafka.kafkaproducerdemo;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
	private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

	public static void main(String[] args) {
		log.info("I am a Kafka consumer");

		String groupId = "my-java-application";
		String topic = "multiplePartitionTopic";

		//	create producer properties
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");

		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());
		properties.setProperty("group.id", groupId);
		properties.setProperty("auto.offset.reset", "earliest");	// can have none(must have consumer group beforehand)/earliest(read from begining)/latest(read from now on)

		//	create a consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		//	subscribe to topic
		consumer.subscribe(Arrays.asList(topic));

		//	poll for data
		while(true){
			log.info("polling");

			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

			for(ConsumerRecord<String, String> record : consumerRecords){
				log.info("Key: " + record.key() + ", Value: " + record.value());
				log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
			}
		}

	}




}