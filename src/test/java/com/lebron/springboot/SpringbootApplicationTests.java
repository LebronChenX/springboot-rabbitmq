package com.lebron.springboot;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.lebron.springboot.amqp.Direct;
import com.lebron.springboot.amqp.Fanout;
import com.lebron.springboot.amqp.Send;
import com.lebron.springboot.amqp.Topic;

@RunWith(SpringRunner.class)
@SpringBootTest(classes={SpringbootApplication.class})
public class SpringbootApplicationTests {

    @Autowired
    private Send send;
    
    @Test
    public void testFanout() throws InterruptedException {
        send.sendMessage(Fanout.FANOUT_EXCHANGE, "lebron", null);
    }

    @Test
	public void testDirect() throws InterruptedException {
        send.sendMessage(Direct.DIRECT_EXCHANGE, "lebron", Direct.DIRECT_ROUTINGKEY);
        Thread.sleep(1000);
	}

	@Test
	public void testTopic() throws InterruptedException {
	    send.sendMessage(Topic.TOPIC_EXCHANGE, "lebron", "abc.lebron");
	    Thread.sleep(1000);
	}

}
