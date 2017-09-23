package com.lebron.springboot.amqp;


import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;  
  
@Configuration  
public class AmqpConfig {  
  
    @Bean  
    public ConnectionFactory connectionFactory() {  
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();  
        connectionFactory.setAddresses("127.0.0.1:5672");  
        connectionFactory.setUsername("guest");  
        connectionFactory.setPassword("guest");  
        connectionFactory.setVirtualHost("/");  
        //完成事件的回调
        connectionFactory.setPublisherConfirms(true);
        return connectionFactory;  
    }  
  
    @Bean  
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)  
    //必须是prototype类型，否则回调策略会选择最后一个  
    public RabbitTemplate rabbitTemplate() {  
        RabbitTemplate template = new RabbitTemplate(connectionFactory());  
        return template;  
    }  
  
}  