package com.kfpanda.mq;

import org.junit.Test;

import com.rabbitmq.client.Channel;

public class RabbitmqUtilTest {
	
	@Test
	public void test1() throws Exception{
		Channel channel;
		
		for(int i = 0; i < 50; i++){
			channel = RabbitmqUtil.borrowObject();
			System.out.println(channel);
			System.out.println("---------------------: " + i);
		}
		
		Thread.sleep(1000000);
	}
}
