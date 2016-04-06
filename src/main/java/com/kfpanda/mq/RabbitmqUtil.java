/*
 * Copyright 2011-2015 10jqka.com.cn All right reserved. This software is the confidential and proprietary information
 * of 10jqka.com.cn (Confidential Information"). You shall not disclose such Confidential Information and shall use it
 * only in accordance with the terms of the license agreement you entered into with 10jqka.com.cn.
 */
package com.kfpanda.mq;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import com.kfpanda.mq.pool.RabbitmqChannelPooledObjectFactory;
import com.kfpanda.mq.pool.RabbitmqConnectionPooledObjectFactory;
import com.kfpanda.util.PropertiesUtil;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kfpanda.core.json.JsonUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

/**
 * 类RabbitmqUtil.java的实现描述：
 * Rabbitmq 公共方法类。
 * 配置文件路径为：classpath: /properties/application.properties
 * @author kfpanda 2015-4-8 上午10:55:45
 */
public class RabbitmqUtil {
	private static Logger logger = LoggerFactory.getLogger(RabbitmqUtil.class);

	private static GenericObjectPool<Channel> pool = null;
	private static final ConnectionFactory factory = new ConnectionFactory();

	static {
		Properties prop = PropertiesUtil.getConfig();
		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		poolConfig.setMaxIdle(Integer.parseInt(prop.getProperty("rabbitmq.channel.pool.max.idle")));
		poolConfig.setMinIdle(Integer.parseInt(prop.getProperty("rabbitmq.channel.pool.min.idle")));
		poolConfig.setMaxTotal(Integer.parseInt(prop.getProperty("rabbitmq.channel.pool.max.total")));
		poolConfig.setMaxWaitMillis(Integer.parseInt(prop.getProperty("rabbitmq.channel.pool.max.wait")));
		// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
		poolConfig.setTestOnBorrow(true);

		pool = new GenericObjectPool<Channel>(new RabbitmqChannelPooledObjectFactory(new RabbitmqConnectionPooledObjectFactory()), poolConfig);

	}
	
	public static Channel borrowObject() {
		try {
			return pool.borrowObject();
		} catch (Exception e) {
			logger.error("rabbitmq borrow channel error.", e);
		}
		return null;
	}
	
	public static void returnObject(Channel obj) {
		pool.returnObject(obj);
	}
	
	public static void main(String[] args) {
		System.out.println("sdsdfsf");
		for(int i = 0; i < 20; i++){
			Channel channel = RabbitmqUtil.borrowObject();
			try {
				channel.basicPublish("direct", "", null, "test----".getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			RabbitmqUtil.returnObject(channel);
//			closeChannel(channel);
			System.out.println(channel + "----------" + i);
		}
		System.out.println("11111111111111");
	}
}
