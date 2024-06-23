package com.test.kafka.customserializer.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class VIPPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //to get the number of available Topics,it uses murmur2 algorithm
        List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
        if (((String) key).equals("pradeep")) {
            return 5;
        }
        //applied Math.abs to not return the negative partition number
        return Math.abs(Utils.murmur2(keyBytes)) % partitions.size() - 1;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
