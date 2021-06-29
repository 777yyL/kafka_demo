package com.hikvision.kafka.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author renpeiqian
 * @date 2021/6/28 16:12
 */

//@Component
public class KafkaConsumer {


    /**
     * 配置过滤策略
     */
    @KafkaListener(topics = {"partopic"},containerFactory = "containerFactory")
    public void getMessage1(List<ConsumerRecord<?, ?>> records){
        records.forEach(record -> {
            System.out.println("简单消费:"+record.topic()+"-"+record.partition()+"-"+record.value());
        });
    }

    /**
     * 批量消费消息
     */
    @KafkaListener(topics = {"partopic"})
    public void getMessage2(List<ConsumerRecord<?, ?>> records){
        System.out.println(">>>批量消费一次，records.size()="+records.size());
        records.forEach(record -> {
            System.out.println("简单消费:"+record.topic()+"-"+record.partition()+"-"+record.value());
        });


    }

    /**
     * 指定消费者消费的topic、partition、以及 initaloffset
     * example: 监听parttopic的分区0和分区1里offset为1开始的消息。
     * @param record
     */
    @KafkaListener(id ="consumer1",groupId = "felix-group",topicPartitions = {
            @TopicPartition(topic = "partopic",partitions = "0",partitionOffsets = @PartitionOffset(partition = "1",initialOffset = "8"))
    })
    public void getMessage2(ConsumerRecord<?,?> record){
        System.out.println("指定消费:"+record.topic()+"-"+record.partition()+"-"+record.value());

    }
}
