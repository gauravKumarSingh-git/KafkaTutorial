package com.kafka.kafkaproducerdemo;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
	private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

	public static void main(String[] args) {
		log.info("I am a Kafka producer");

		//	create producer properties
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());

//		properties.setProperty("num.partitions", "3");
//		properties.setProperty("batch.size", "400");
//		properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

		//	create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		//	Sticky Partitioner example - send record as batch  - same partition
		for(int j = 0; j < 10; j++) {
			for (int i = 0; i < 30; i++) {
				//	create a producer record
				ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello World");

				//	send data
				producer.send(producerRecord, new Callback() {
					@Override
					public void onCompletion(RecordMetadata recordMetadata, Exception e) {
						if (e == null) {
							log.info("Received new metadata \n" +
									"Topic: " + recordMetadata.topic() + "\n" +
									"Partition: " + recordMetadata.partition() + "\n" +
									"Offset: " + recordMetadata.offset() + "\n" +
									"Timestamp: " + recordMetadata.timestamp() + "\n");
						} else {
							log.error("Error while producing", e);
						}
					}
				});

			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		//	flush and close the producer
		//	tell the producer to send all data and block until done --synchronous
		producer.flush();

		//	producer.close() will also call flush() implicitly
		producer.close();
	}




}