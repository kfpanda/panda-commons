package com.kfpanda.mq;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.rabbitmq.client.Channel;

import java.io.IOException;

public class RabbitmqUtilTest {
	private static Logger logger = LogManager.getLogger(RabbitmqUtilTest.class);

	@Test
	public void borrowObject() throws Exception{
		Channel channel;
		
		for(int i = 0; i < 50; i++){
			channel = RabbitmqUtil.borrowObject();
			System.out.println(channel);
			System.out.println("---------------------: " + i);
		}
		
		Thread.sleep(1000000);
	}

	@Test
	public void pubMsg() throws InterruptedException {
		RabbitmqUtil.returnObject(RabbitmqUtil.borrowObject());
		long start = System.currentTimeMillis();
		for(int i = 0; i < 20000; i++){
			Channel channel = RabbitmqUtil.borrowObject();
			try {
				channel.exchangeDeclare("amq.direct", "direct", true);
				channel.basicPublish("amq.direct", "test", null, "test----".getBytes());
			} catch (IOException e) {
				logger.error("", e);
			}
			RabbitmqUtil.returnObject(channel);
//			closeChannel(channel);
//			System.out.println(channel + "----------" + i);
		}
		long end = System.currentTimeMillis();
		System.out.println(end - start);
//		Thread.sleep(10000);
	}

}
