package com.lebron.springboot.amqp;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.rabbitmq.client.Channel;

@Configuration
public class Direct {
    
    public static final String DIRECT_EXCHANGE   = "direct-exchange";  
    public static final String DIRECT_ROUTINGKEY = "direct-routingKey";  

    @Autowired
    private AmqpConfig amqpConfig;
    
    //配置direct交换机绑定策略
    @Bean  
    public DirectExchange directExchange() {  
        return new DirectExchange(DIRECT_EXCHANGE);  
    }  
  
    @Bean  
    public Queue directQueue() {  
        return new Queue("direct-queue", true); //队列持久  
    }  
  
    @Bean  
    public Binding directBinding() {  
        return BindingBuilder.bind(directQueue()).to(directExchange()).with(DIRECT_ROUTINGKEY);  
    }  
  
    @Bean  
    public SimpleMessageListenerContainer directMessageContainer() {  
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(amqpConfig.connectionFactory());  
        container.setQueues(directQueue());  
        container.setExposeListenerChannel(true);  
        container.setMaxConcurrentConsumers(1);  
        container.setConcurrentConsumers(1);  
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL); //设置确认模式手工确认  NONE,AUTO
        container.setMessageListener(new ChannelAwareMessageListener() {  
  
            @Override  
            public void onMessage(Message message, Channel channel) throws Exception {  
                byte[] body = message.getBody();  
                System.out.println("direct receive msg : " + new String(body));
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
