package com.lebron.springboot.amqp;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.rabbitmq.client.Channel;

@Configuration
public class Fanout {
    
    public static final String FANOUT_EXCHANGE   = "fanout-exchange";  
    
    @Autowired
    private AmqpConfig amqpConfig;
    
    @Bean  
    public FanoutExchange fanoutExchange() {  
        return new FanoutExchange(FANOUT_EXCHANGE);  
    }  
    
    //配置fanout交换机绑定策略一
    @Bean  
    public Queue fanoutQueue() {  
        return new Queue("fanout-queue", true); //队列持久  
    }  

    @Bean  
    public Binding fanoutBinding() {  
        return BindingBuilder.bind(fanoutQueue()).to(fanoutExchange());
    }  
    
    //置topic交换机绑定策略二
    @Bean  
    public Queue fanoutQueue2() {  
        return new Queue("fanout-queue2", true); //队列持久  
    } 
    
    @Bean  
    public Binding fanoutBinding2() {  
        return BindingBuilder.bind(fanoutQueue2()).to(fanoutExchange());
    } 

    @Bean  
    public SimpleMessageListenerContainer fanoutMessageContainer() {  
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(amqpConfig.connectionFactory());  
        container.setQueues(fanoutQueue(),fanoutQueue2());
        container.setExposeListenerChannel(true);  
        container.setMaxConcurrentConsumers(1);  
        container.setConcurrentConsumers(1);  
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL); //设置确认模式手工确认  NONE,AUTO
        container.setMessageListener(new ChannelAwareMessageListener() {  
            
            @Override  
            public void onMessage(Message message, Channel channel) throws Exception {  
                byte[] body = message.getBody();  
                System.out.println("fanout receive msg : " + new String(body));
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
