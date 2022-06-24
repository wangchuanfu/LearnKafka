package com.lun.kafka.producer.wangchuanfu.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub

	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		// TODO Auto-generated method stub
//		List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
		Integer integer = cluster.partitionCountForTopic(topic);
//		return key.toString().hashCode()%integer;
		return 0;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
