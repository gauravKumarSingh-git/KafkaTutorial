package com.kafka.kafkaproducerdemo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka producer");

        //	create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

//		properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
//        properties.setProperty("num.partitions", "3");


        //	create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j = 0; j < 2; j++){
            for (int i = 0; i < 10; i++) {
                String topic = "multiplePartitionTopic";
                String key = "id_" + i;
                String value = "hello world " + i;

                //	create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                //	send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            log.info("Received new metadata \n" +
                                    "Key: " + key + " | Partition: " + recordMetadata.partition());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });

            }

        }



        //	flush and close the producer
        //	tell the producer to send all data and block until done --synchronous
        producer.flush();

        //	producer.close() will also call flush() implicitly
        producer.close();
    }




}