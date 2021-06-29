package com.hikvision.kafka.config;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author renpeiqian
 * @date 2021/6/28 16:49
 * 配置自定义分区器
 */
public class CustomizePartitioner implements Partitioner {

    //方法返回值就表示将消息发送到几号分区
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        return 0;
    }

    @Override
    public void close() {

    }


    @Override
    public void configure(Map<String, ?> map) {

    }
}
