package com.lebron.springboot.amqp;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.rabbitmq.client.Channel;

@Configuration
public class Topic {
    
    public static final String TOPIC_EXCHANGE   = "topic-exchange";  
    //"*"与"#"，用于做模糊匹配，其中"*"用于匹配一个单词，“#”用于匹配多个单词（可以是零个）
    public static final String TOPIC_ROUTINGKEY1 = "*.lebron"; 
    public static final String TOPIC_ROUTINGKEY2 = "abc.*"; 
    
    @Autowired
    private AmqpConfig amqpConfig;
    
    
    @Bean  
    public TopicExchange topicExchange() {  
        return new TopicExchange(TOPIC_EXCHANGE);  
    }  
    
    //配置topic交换机绑定策略一
    @Bean  
    public Queue topicQueue() {  
        return new Queue("topic-queue", true); //队列持久  
    }  
    
    @Bean  
    public Binding topicBinding() {  
        return BindingBuilder.bind(topicQueue()).to(topicExchange()).with(TOPIC_ROUTINGKEY1);
    }  
    
    @Bean  
    public Queue topicQueue2() {  
        return new Queue("topic-queue2", true); //队列持久  
    } 
    
    //置topic交换机绑定策略二
    @Bean  
    public Binding topicBinding2() {  
        return BindingBuilder.bind(topicQueue2()).to(topicExchange()).with(TOPIC_ROUTINGKEY2);
    } 

    //监听
    @Bean  
    public SimpleMessageListenerContainer topicMessageContainer() {  
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(amqpConfig.connectionFactory());  
        container.setQueues(topicQueue(),topicQueue2());
        container.setExposeListenerChannel(true);  
        container.setMaxConcurrentConsumers(1);  
        container.setConcurrentConsumers(1);  
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL); //设置确认模式手工确认  NONE,AUTO
        container.setMessageListener(new ChannelAwareMessageListener() {  
            
            @Override  
            public void onMessage(Message message, Channel channel) throws Exception {  
                byte[] body = message.getBody();  
                System.out.println("topic receive msg : " + new String(body));
                boolean success = true;
                if ( success ) {
                    //确认成功，使用basicAck，并告诉mq将消息从队列中删除
                    channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);   
                } else {
                    //消费失败，使用basicNack，并告诉mq将消息再次放入队列
                    channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
                }
            }  
        });  
        return container;  
    }  
    
}
