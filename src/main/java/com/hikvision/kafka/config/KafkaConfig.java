package com.hikvision.kafka.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareErrorHandler;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;

/**
 * @author renpeiqian
 * @date 2021/6/29 9:22
 */

@Configuration
public class KafkaConfig {

    @Autowired
    ConsumerFactory consumerFactory;

    //配置消费者异常处理器
    @Bean
    public ConsumerAwareListenerErrorHandler consumerAwareErrorHandler(){
      return (message, e, consumer) -> {
          System.out.println("消费异常："+message.getPayload());
          return null;
      };

    }

    /**
     * 配置监听器工厂，设置禁止kafkaListener自启动并注入容器
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory delaycontainerFactory(){
        ConcurrentKafkaListenerContainerFactory containerFactory = new ConcurrentKafkaListenerContainerFactory();
        containerFactory.setConsumerFactory(consumerFactory);
        //禁止KafkaListener自启动
        containerFactory.setAutoStartup(false);
        return containerFactory;
    }

    /**
     * 配置监听器工厂并设置过滤规则
     */
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory containerFactory(){
//        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
//        factory.setConsumerFactory(consumerFactory);
//        //被过滤的消息将被丢弃
//        factory.setAckDiscarded(true);
//        // 消息过滤策略
//       factory.setRecordFilterStrategy(consumerRecord -> {
//           //过滤奇数，接收偶数
//           if (Integer.parseInt(consumerRecord.value().toString()) % 2==0){
//                return false;
//           }
//           //返回true消息则被过滤
//           return true;
//       });
//        return factory;
//    }
}
