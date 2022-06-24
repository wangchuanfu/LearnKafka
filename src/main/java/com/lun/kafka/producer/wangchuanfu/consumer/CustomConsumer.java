package com.lun.kafka.producer.wangchuanfu.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 
 * 
 * @author
 *
 */
public class CustomConsumer {
	public static void main(String[] args) {
		
		Properties props = new Properties();
		
		props.put("bootstrap.servers", "127.0.0.1:9092");
//		props.put("enable.auto.commit", "true");//自动提交
		props.put("enable.auto.commit", "false");//非自动提交
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		
		props.put("group.id", "abcd");//消费者组
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//--from-beginning
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		
		consumer.subscribe(Arrays.asList("bigdata"));//订阅topic
		
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
			}
		}
	}
}
