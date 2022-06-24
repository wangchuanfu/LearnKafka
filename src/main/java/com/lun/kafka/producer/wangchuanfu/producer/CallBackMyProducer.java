package com.lun.kafka.producer.wangchuanfu.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CallBackMyProducer {
    public static void main(String[] args) {
        Properties props=new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // 重试次数
        props.put("retries", 1);
        // 批次大小
        props.put("batch.size", 16384);
        // 等待时间
        props.put("linger.ms", 1);
        // RecordAccumulator 缓冲区大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
//            producer.send(new ProducerRecord<String, String>("bigdata", "bigdata - 1"), new Callback() {//不指定分区
            //指定分区
           // producer.send(new ProducerRecord<String, String>("bigdata", 0,"bigdata","bigdata - 1"+i), new Callback() {
          //  指定key,哈希
              producer.send(new ProducerRecord<String, String>("bigdata", 0,"bigdata","bigdata - 1"+i), new Callback() {

                            @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {

                    if (exception == null) {
                        System.out.println(metadata.partition() + " - " + metadata.offset());
                    }else{
                        exception.printStackTrace();
                    }
                }
            });
        }
        producer.close();//关闭连接
    }
}
