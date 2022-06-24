package com.lun.kafka.producer.wangchuanfu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class InterceptorProducer {
	public static void main(String[] args) {
		// 1 设置配置信息
		Properties props = new Properties();
		props.put("bootstrap.servers", "127.0.0.1:9092");
		props.put("acks", "all");
		props.put("retries", 3);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		
		// 构建拦截器(可以是多个)
		List<String> interceptors = new ArrayList<>();
		interceptors.add("com.lun.kafka.interceptor.TimeInterceptor");
		interceptors.add("com.lun.kafka.interceptor.CounterInterceptor");
		
		props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
		
		String topic = "bigdata";
		Producer<String, String> producer = new KafkaProducer<>(props);
		// 3 发送消息
		for (int i = 0; i < 10; i++) {
			ProducerRecord<String, String> record = new ProducerRecord<>(topic, "message" + i);
			producer.send(record);
		}
		
		// 4 一定要关闭 producer，这样才会调用 interceptor 的 close 方法
		producer.close();
		
	}
}
