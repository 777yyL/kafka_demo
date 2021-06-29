package com.hikvision.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author renpeiqian
 * @date 2021/6/28 16:11
 */
@RestController
@RequestMapping("/kafka")
public class KafkaProducer {

    @Autowired
    KafkaTemplate<String,Object> kafkaTemplate;

    /**
     * 批量发送消息
     */
    @RequestMapping("/sendbatch/{message}")
    public void sendMessage(@PathVariable String message){
        for (int i=0;i<20;i++){
            String msg=i+"";
            System.out.println(msg);
            kafkaTemplate.send("partopic",msg);
        }

    }

    
    /**
     * 带有回调的生产者发送消息
     * @param callbackMessage
     */
    @GetMapping("/send/{callbackMessage}")
    public void sendMessage2(@PathVariable String callbackMessage){
        kafkaTemplate.send("partopic",callbackMessage).addCallback(sucess -> {
            //消息发送到topic
            String topic = sucess.getRecordMetadata().topic();
            //消息发送到的分区
            int partition = sucess.getRecordMetadata().partition();
            //消息相互于分区的offset
            long offset= sucess.getRecordMetadata().offset();
            System.out.println("发送消息成功:" + topic + "-" + partition + "-" + offset);
        },failure -> {
            System.out.println("发送消息失败:" + failure.getMessage());
        });
    }

    /**
     * kafka事务提交
     */
    @GetMapping("/transaction")
    public void sendTransMessage(){
        kafkaTemplate.executeInTransaction(op ->{
            op.send("partopic","transactionMeassage");
            throw new RuntimeException("消息发送失败，事务回滚");
        });
    }
}
