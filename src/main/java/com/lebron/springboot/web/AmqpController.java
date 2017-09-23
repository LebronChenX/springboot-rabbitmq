package com.lebron.springboot.web;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.lebron.springboot.amqp.Direct;
import com.lebron.springboot.amqp.Fanout;
import com.lebron.springboot.amqp.Send;
import com.lebron.springboot.amqp.Topic;

@RestController
public class AmqpController {

    @Autowired
    private Send send;
    
    @RequestMapping("/fanout/{content}")
    public void testFanout(@PathVariable String content) throws InterruptedException {
        send.sendMessage(Fanout.FANOUT_EXCHANGE, content, null);
    }

    @RequestMapping("/direct/{content}")
    public void testDirect(@PathVariable String content) throws InterruptedException {
        send.sendMessage(Direct.DIRECT_EXCHANGE, content, Direct.DIRECT_ROUTINGKEY);
    }

    @RequestMapping("/topic/{content}")
    public void testTopic(@PathVariable String content) throws InterruptedException {
        send.sendMessage(Topic.TOPIC_EXCHANGE, content, "abc.lebron");
    }
}
