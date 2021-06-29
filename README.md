# kafka_demo

##### 特性

Apache Kafka 是一款开源的消息引擎系统。（引擎：能力转换； 消息引擎：将上游的巨大能力平稳的输出到下游）

消息引擎系统是一组规范。企业利用这组规范在不同系统之间传递语义准确的消息，实现松耦合的异步式数据传递。通俗来说：系统 A 发送消息给消息引擎系统，系统 B 从消息引擎系统中读取 A 发送的消息。

消息引擎系统传递的是消息。在设计时我们其实从从两方面考虑就行。

==1.如何设计传递消息的格式？ ==

它使用的是纯二进制的字节序列。当然消息还是结构化的，只是在使用之前都要将其转换成二进制的字节序列。

==2.消息传递的机制? 传输协议？==

- **点对点模型**：也叫消息队列模型。系统 A 发送的消息只能被系统 B 接收，其他任何系统都不能读取 A 发送的消息。
- **发布 / 订阅模型**：与上面不同的是，它有一个主题（Topic）的概念，你可以理解成逻辑语义相近的消息容器。该模型也有发送方和接收方，只不过提法不同。发送方也称为发布者（Publisher），接收方称为订阅者（Subscriber）。和点对点模型不同的是，这个模型可能存在多个发布者向相同的主题发送消息，而订阅者也可能存在多个，它们都能接收到相同主题的消息。生活中的报纸订阅就是一种典型的发布 / 订阅模型。

##### 工作机制

在 Kafka 中，发布订阅的对象是主题（Topic），你可以为每个业务、每个应用甚至是每类数据都创建专属的主题。

**Record**

消息。Kafka 是消息引擎，这里的消息就是指 Kafka 处理的主要对象。

**Topic**

主题是承载消息的逻辑容器，在实际使用中多用来区分具体的业务。

**Producer and Consumer**

向主题发布消息的客户端应用程序称为生产者（Producer），生产者程序通常持续不断地向一个或多个主题发送消息，而订阅这些主题消息的客户端应用程序就被称为消费者（Consumer）。和生产者类似，消费者也能够同时订阅多个主题的消息。我们把生产者和消费者统称为客户端（Clients）。

**Broker**

Kafka 的服务器端由被称为 Broker 的服务进程构成，==即一个 Kafka 集群由多个 Broker 组成，Broker 负责接收和处理客户端发送过来的请求，以及对消息进行持久化。==

虽然多个 Broker 进程能够运行在同一台机器上，但更常见的做法是将不同的 Broker 分散运行在不同的机器上，这样如果集群中某一台机器宕机，即使在它上面运行的所有 Broker 进程都挂掉了，其他机器上的 Broker 也依然能够对外提供服务。这其实就是 Kafka 提供高可用的手段之一。

**Replication**

实现高可用的另一个手段就是备份机制（Replication）。

就是将相同的数据拷贝到多台机器上。这些相同的数据拷贝在Kafka中被称为副本（Replica）。

副本的数量是可以配置的，这些副本保存着相同的数据，但有着不同的角色和作用。

**Leader Replica、Follower Replica**

Kafka 定义了两类副本：领导者副本（Leader Replica）和追随者副本（Follower Replica）。

==前者对外提供服务，这里的对外指的是与客户端程序进行交互；而后者只是被动地追随领导者副本而已，不能与外界进行交互。==

副本工作机制：

**生产者总是向领导者副本写消息；而消费者总是从领导者副本读消息。**

**至于追随者副本，它只做一件事：向领导者副本发送请求，请求领导者把最新生产的消息发给它，这样它能保持与领导者的同步。**

副本机制可以保证数据的持久化和消息不丢失。

**Partitioning**

Kafka 中的分区机制指的是将每个主题划分成多个分区（Partition），每个分区是一组有序的消息日志。生产者生产的每条消息只会被发送到一个分区中。

**Offset**

生产者向分区写入消息，每条消息在分区中的位置信息由一个叫位移（Offset）的数据来表征。分区位移总是从 0 开始，假设一个生产者向一个空分区写入了 10 条消息，那么这 10 条消息的位移依次是 0、1、2、......、9。

**Consumer Group**

​	==多个消费者实例共同组成的一个组，同时消费多个分区以实现高吞吐。==

**Rebalance**

重平衡。消费者组内某个消费者实例挂掉后，其他消费者实例自动重新分配订阅主题分区的过程。Rebalance 是 Kafka 消费者端实现高可用的重要手段。

**三层消息架构**

![image-20210628104622127](D:\workspace\springboot_kafka\README.assets\image-20210628104622127.png)

- 第一层是主题层，每个主题可以配置M个分区，而每个分区又可以分配配置N个副本。
- 第二层是分区层，每个分区的N个副本中只能有一个充当领导者角色对外提供服务；其他N-1个副本是追随者副本，只能提供数据冗余之用。
- 第三层是消息层，分区中包含若干条消息每条消息的位移从0开始，依次递增。
- 客户端程序只能与分区中的领导者副本进行交互。

##### 持久化

Kafka 使用消息日志（Log）来保存数据，一个日志就是磁盘上一个只能追加写（Append-only）消息的物理文件。

Kafka 底层，一个日志又进一步细分成多个日志段，消息被追加写到当前最新的日志段中，当写满了一个日志段后，Kafka 会自动切分出一个新的日志段，并将老的日志段封存起来。Kafka 在后台还有定时任务会定期地检查老的日志段是否能够被删除，从而实现回收磁盘空间的目的。



##### 安装

```bash
# 拉取镜像
docker pull wurstmeister/zookeeper
docker pull wurstmeister/kafka

# 启动zookeeper
docker run -d --name zookeeper -p 2181:2181 -v /etc/localtime:/etc/localtime wurstmeister/zookeeper

# 启动kakfa
docker run  -d --name kafka -p 9092:9092 -e KAFKA_BROKER_ID=0 -e KAFKA_ZOOKEEPER_CONNECT=172.21.135.24:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://172.21.135.24:9092 -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 -v /etc/localtime:/etc/localtime  wurstmeister/kafka

docker run  -d --name kafka -p 9092:9092 
-e KAFKA_BROKER_ID=0  #在集群中指定brokerid进行区分 
-e KAFKA_ZOOKEEPER_CONNECT=10.9.44.11:2181 # 配置zookeeper 管理kafka的路径
-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://10.9.44.11:9092  # 将kafka注册到zookeeper
-e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 # 配置kafka的监听端口
-v /etc/localtime:/etc/localtime  #容器时间同步虚拟机的时间
-t wurstmeister/kafka

# 进入容器
docker exec -it kafka /bin/sh

# 进入kafka所在目录
cd opt/kafka_2.12-1.1.0/

# 集群搭建
# 使用docker命令可快速在同一台机器搭建多个kafka，只需要改变brokerId和端口
docker run  -d --name kafka_salve -p 9093:9093 -e KAFKA_BROKER_ID=1 -e KAFKA_ZOOKEEPER_CONNECT=172.21.135.24:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://172.21.135.24:9093 -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9093 -v /etc/localtime:/etc/localtime  wurstmeister/kafka

#  创建Replication为2，Partition为2的topic
bin/kafka-topics.sh --create --zookeeper 172.21.135.24:2181 --replication-factor 2 --partitions 2 --topic partopic

Created topic partopic.

# 查看topic 的状态
bin/kafka-topics.sh --describe --zookeeper  172.21.135.24:2181 --topic partopic

Topic: partopic	PartitionCount: 2	ReplicationFactor: 2	Configs: 
	Topic: partopic	Partition: 0	Leader: 0	Replicas: 0,1	Isr: 0,1
	Topic: partopic	Partition: 1	Leader: 1	Replicas: 1,0	Isr: 1,0


```

##### springboot 集成 kafka

**入门**

1.引入依赖

```xml
   <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
   </dependency>
```

2.配置

```properties
###########【Kafka集群】###########
spring.kafka.bootstrap-servers=47.95.37.191:9092,47.95.37.191:9093
###########【初始化生产者配置】###########
# 重试次数
spring.kafka.producer.retries=0
# 应答级别:多少个分区副本备份完成时向生产者发送ack确认(可选0、1、all/-1)
spring.kafka.producer.acks=1
# 批量大小
spring.kafka.producer.batch-size=16384
# 提交延时
spring.kafka.producer.properties.linger.ms=0
# 当生产端积累的消息达到batch-size或接收到消息linger.ms后,生产者就会将消息提交给kafka
# linger.ms为0表示每接收到一条消息就提交给kafka,这时候batch-size其实就没用了
?
# 生产端缓冲区大小
spring.kafka.producer.buffer-memory = 33554432
# Kafka提供的序列化和反序列化类
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer
# 自定义分区器
# spring.kafka.producer.properties.partitioner.class=com.felix.kafka.producer.CustomizePartitioner
?
###########【初始化消费者配置】###########
# 默认的消费组ID
spring.kafka.consumer.properties.group.id=defaultConsumerGroup
# 是否自动提交offset
spring.kafka.consumer.enable-auto-commit=true
# 提交offset延时(接收到消息后多久提交offset)
spring.kafka.consumer.auto.commit.interval.ms=1000
# 当kafka中没有初始offset或offset超出范围时将自动重置offset
# earliest:重置为分区中最小的offset;
# latest:重置为分区中最新的offset(消费分区中新产生的数据);
# none:只要有一个分区不存在已提交的offset,就抛出异常;
spring.kafka.consumer.auto-offset-reset=latest
# 消费会话超时时间(超过这个时间consumer没有发送心跳,就会触发rebalance操作)
spring.kafka.consumer.properties.session.timeout.ms=120000
# 消费请求超时时间
spring.kafka.consumer.properties.request.timeout.ms=180000
# Kafka提供的序列化和反序列化类
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
# 消费端监听的topic不存在时，项目启动会报错(关掉)
spring.kafka.listener.missing-topics-fatal=false
# 设置批量消费
# spring.kafka.listener.type=batch
# 批量消费每次最多消费多少条消息
# spring.kafka.consumer.max-poll-records=50
```

3.创建topic

kafka 在发送消息时，会自动帮我们创建topic，但是这种情况下创建的topic默认只有一个分区，且没有副本。所以我们可以手动创建或者在项目中新建一个配置类专门来初始化topic。

```java
@Configuration
public class KafkaInitialConfiguration {
    // 创建一个名为testtopic的Topic并设置分区数为8，分区副本数为2
    @Bean
    public NewTopic initialTopic() {
        return new NewTopic("testtopic",8, (short) 2 );
    }

     // 如果要修改分区数，只需修改配置值重启项目即可
    // 修改分区数并不会导致数据的丢失，但是分区数只能增大不能减小
    @Bean
    public NewTopic updateTopic() {
        return new NewTopic("testtopic",10, (short) 2 );
    }

```

4.创建生产者、消费者

```java
/**
 * @author renpeiqian
 * @date 2021/6/28 16:11
 */
@RestController
@RequestMapping("/kafka")
public class KafkaProducer {

    @Autowired
    KafkaTemplate<String,Object> kafkaTemplate;

    //发送消息
    @RequestMapping("/send/{message}")
    public void sendMessage(@PathVariable String message){
        kafkaTemplate.send("partopic",message);
    }
}
```

消费者

```java
/**
 * @author renpeiqian
 * @date 2021/6/28 16:12
 */

@Component
public class KafkaConsumer {

    //监听消息
    @KafkaListener(topics = {"partopic"})
    public void getMessage(ConsumerRecord<?,?> record){
        System.out.println("简单消费:"+record.topic()+"-"+record.partition()+"-"+record.value());

    }
}

```

##### 生产者

**1.创建带有回调的生产者。**

kafkaTemplate提供了一个回调方法addCallback，我们可以在回调方法中监控消息是否发送成功 或 失败时做补偿处理。

```java
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
```



**2.自定义分区器**

kafka中每个topic被划分为多个分区，分区中又有n个副本。还存放消息。生产者将消息发送给topic，具体到那个分区。有一定的分区策略。

kafka为我们提供默认的分区策略，也支持我们自定义分区策略。路由机制：

- 若发送消息时指定了分区（及自定义分区策略），则直接将消息追加的直接分区。
- 若发送消息时未指定Partition，但指定了key（kafka允许为每条消息设置一个key），则对key值进行hash计算，根据计算结果路由到指定分区，这种情况下可以保证同一个key的所有消息都进入到相同的分区。
- partition和key都未指定，则遵循kafka默认的分区策略。**轮询**

自定义分区策略实现方式：

1.新建一个分区器实现Partitioner接口，重写方法。

```java
/**
 * @author renpeiqian
 * @date 2021/6/28 16:49
 */
public class CustomizePartitioner implements Partitioner {

    //方法返回值就表示将消息发送到几号分区
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        return 1;
    }

    @Override
    public void close() {

    }


    @Override
    public void configure(Map<String, ?> map) {

    }
}
```

2.引入配置

```properties
#自定义分区器
spring.kafka.producer.properties.partitioner.class=com.hikvision.kafka.config.CustomizePartitioner
```

3.kafka事务提交

==如果在发送消息时需要创建事务，可以使用 KafkaTemplate 的 executeInTransaction 方法来声明事务==

##### 消费者

**点对点模型**

点对点即同一条消息只能被下游的一个消费者消费，其他消费者不能消费。

在 Kafka 中实现这种 P2P 模型的方法就是引入了**消费者组（Consumer Group）**。

==消费者组==，指的就是多个消费者实例共同组成一个组来消费一组主题。==多个消费者实例共同组成的一个组，同时消费多个分区以实现高吞吐。==

消费者组里面的所有消费者实例不仅“瓜分”订阅主题的数据，而且更酷的是它们还能彼此协助。假设组内某个实例挂掉了，Kafka 能够自动检测到，然后把这个 Failed 实例之前负责的分区转移给其他活着的消费者。

作用：主要是为了提升消费者端的吞吐量。**多个消费者实例同时消费**，加速整个消费端的吞吐量（TPS）。

**消费者位移**

每个消费者在消费消息的过程中必然需要有个字段记录它当前消费到了分区的哪个位置上，这个字段就是消费者位移（Consumer Offset）。

1.指定具体的topic、paraition、offset进行消费。

```java
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


```

① id：消费者ID；

② groupId：消费组ID；

③ topics：监听的topic，可监听多个；

④ topicPartitions：可配置更加详细的监听信息，可指定topic、parition、offset监听

**2.批量消费**

1.开启配置

```properties
 #设置批量消费
 spring.kafka.listener.type=batch
 #批量消费每次最多消费多少条消息
 spring.kafka.consumer.max-poll-records=50
```

2.发送消息

```java
  @RequestMapping("/send/{message}")
    public void sendMessage(@PathVariable String message){
        for (int i=0;i<20;i++){
            String msg="第"+i+"条消息被发送, "+message;
            kafkaTemplate.send("partopic",msg);
        }

    }
```

3.接收消息用list接收

```java
  @KafkaListener(topics = {"partopic"})
    public void getMessage(List<ConsumerRecord<?, ?>> records){
        System.out.println(">>>批量消费一次，records.size()="+records.size());
        records.forEach(record -> {
            System.out.println("简单消费:"+record.topic()+"-"+record.partition()+"-"+record.value());
        });
    }
```

**ConsumerAwareListenerErrorHandler 异常处理器**

==通过异常处理器，我们可以处理consumer在消费时发生的异常。==

1.新建一个 ConsumerAwareListenerErrorHandler 类型的异常处理方法，用@Bean注入，BeanName默认就是方法名

```java
 @Bean
    public ConsumerAwareListenerErrorHandler consumerAwareErrorHandler(){
      return (message, e, consumer) -> {
          System.out.println("消费异常："+message.getPayload());
          return null;
      };

    }
```



2.然后我们将这个异常处理器的BeanName放到@KafkaListener注解的errorHandler属性里面，当监听抛出异常的时候，则会自动调用异常处理器，

```java
@KafkaListener(topics = {"partopic"},errorHandler = "consumerAwareErrorHandler")
    public void getMessage(ConsumerRecord<?, ?> record){
        System.out.println("指定消费:"+record.topic()+"-"+record.partition()+"-"+record.value());
        throw  new RuntimeException("模拟消费者异常");
    }
```

**4、消息过滤器**

消息过滤器可以在消息抵达Consumer之前被拦截，在实际应用中，我们可以根据自己的业务逻辑，筛选出重要的信息再交给KafkaListener处理，不需要的消息过滤掉。

实现方式：

1.配置消息过滤只需要为 监听器工厂 配置一个RecordFilterStrategy（消息过滤策略），返回true的时候消息将会被抛弃，返回false时，消息能正常抵达监听容器。

```java
  @Bean
    public ConcurrentKafkaListenerContainerFactory containerFactory(){
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory);
        //被过滤的消息将被丢弃
        factory.setAckDiscarded(true);
        // 消息过滤策略
       factory.setRecordFilterStrategy(consumerRecord -> {
           //过滤奇数，接收偶数
           if (Integer.parseInt(consumerRecord.value().toString())%2==0){
                return false;
           }
           //返回true消息则被过滤
           return true;
       });
        return factory;
    }
```

2.给消费者配置过滤策略

```java
 @KafkaListener(topics = {"partopic"},containerFactory = "containerFactory")
    public void getMessage(List<ConsumerRecord<?, ?>> records){
        records.forEach(record -> {
            System.out.println("简单消费:"+record.topic()+"-"+record.partition()+"-"+record.value());
        });
    }
```

**5、消息转发**

在实际开发中，我们可能有这样的需求，应用A从TopicA获取到消息，经过处理后转发到TopicB，再由应用B监听处理消息，即一个应用处理完成后将该消息转发至其他应用，完成消息的转发。

在SpringBoot集成Kafka实现消息的转发也很简单，只需要通过一个@SendTo注解，被注解方法的return值即转发的消息内容.



**6.定时启动、停止监听器**

默认情况下，当消费者项目启动的时候，监听器就开始工作，监听消费发送到指定topic的消息，那如果我们不想让监听器立即工作，想让它在我们指定的时间点开始工作，或者在我们指定的时间点停止工作，该怎么处理呢——使用KafkaListenerEndpointRegistry，下面我们就来实现：

① 禁止监听器自启动；

② 创建两个定时任务，一个用来在指定时间点启动定时器，另一个在指定时间点停止定时器；

1.新建一个定时任务类，用注解@EnableScheduling声明，KafkaListenerEndpointRegistry 在SpringIO中已经被注册为Bean，直接注入，设置禁止KafkaListener自启动，

```java
@EnableScheduling
@Component
public class CronTimer {

    @Autowired
    KafkaListenerEndpointRegistry registry;



    // 监听器
    @KafkaListener(id="timingConsumer",topics = "partopic",containerFactory = "containerFactory")
    public void onMessage1(ConsumerRecord<?, ?> record){
        System.out.println("消费成功："+record.topic()+"-"+record.partition()+"-"+record.value());
    }

    // 定时启动监听器
    @Scheduled(cron = "0 43 10 * * ? ")
    public void startListener() {
        System.out.println("启动监听器...");
        // "timingConsumer"是@KafkaListener注解后面设置的监听器ID,标识这个监听器
        if (!registry.getListenerContainer("timingConsumer").isRunning()) {
            registry.getListenerContainer("timingConsumer").start();
        }
        //registry.getListenerContainer("timingConsumer").resume();
    }

    // 定时停止监听器
    @Scheduled(cron = "0 44 10 * * ? ")
    public void shutDownListener() {
        System.out.println("关闭监听器...");
        registry.getListenerContainer("timingConsumer").pause();
    }


}
```

2.设置kafkaListener禁止自启动

```java
  /**
     * 配置监听器工厂，设置禁止kafkaListener自启动并注入容器
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory containerFactory(){
        ConcurrentKafkaListenerContainerFactory containerFactory = new ConcurrentKafkaListenerContainerFactory();
        containerFactory.setConsumerFactory(consumerFactory);
        //禁止KafkaListener自启动
        containerFactory.setAutoStartup(false);
        return containerFactory;
    }
```

