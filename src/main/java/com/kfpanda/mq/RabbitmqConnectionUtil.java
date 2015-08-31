/*
 * Copyright 2011-2015 10jqka.com.cn All right reserved. This software is the confidential and proprietary information
 * of 10jqka.com.cn (Confidential Information"). You shall not disclose such Confidential Information and shall use it
 * only in accordance with the terms of the license agreement you entered into with 10jqka.com.cn.
 */
package com.kfpanda.mq;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kfpanda.core.FilePath;
import com.kfpanda.mq.pool.RabbitmqPool;
import com.kfpanda.mq.pool.PoolConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * 类RabbitmqUtil.java的实现描述：
 * Rabbitmq 公共方法类。
 * 配置文件路径为：classpath: /properties/application.properties
 * @author kfpanda 2015-4-8 上午10:55:45
 */
public class RabbitmqConnectionUtil {
	private static Logger logger = LoggerFactory.getLogger(RabbitmqConnectionUtil.class);
	private static final String configFilePath = "/properties/application.properties";
	private static RabbitmqPool pool = null;
	private static Connection conn;
	private static final String AMQ_DIRECT = "amq.direct";
	private static final String EXCHANGE_TYPE = "direct";
	private static final String ROUTE_KEY = "route.key";
	private static String amqDirect;
	private static String exchangeType;
	private static String routeKey;
	
	static {
		Properties prop = new Properties();
		InputStream in = null;
		try {
			in = readProperties();
			prop.load(in);
		} catch (IOException e) {
			logger.error("rabbitmq配置文件载入失败：", e);
		}finally{
			IOUtils.closeQuietly(in);
		}
		PoolConfig poolConfig = new PoolConfig();
		poolConfig.setMaxActive(10);
		poolConfig.setMaxIdle(5);
		poolConfig.setMinIdle(2);
		poolConfig.setMaxWait(1000 * 100);
		// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
		poolConfig.setTestOnBorrow(true);
		String timeout = prop.getProperty("rabbitmq.timeout");
		amqDirect = prop.getProperty("rabbitmq.amq.direct");
		exchangeType = prop.getProperty("rabbitmq.exchange.type");
		routeKey = prop.getProperty("rabbitmq.route.key");
		amqDirect = amqDirect == null ? AMQ_DIRECT : amqDirect;
		exchangeType = exchangeType == null ? EXCHANGE_TYPE : exchangeType;
		routeKey = routeKey == null ? ROUTE_KEY : routeKey;
		
		pool = new RabbitmqPool(poolConfig, prop.getProperty("rabbitmq.host"),
					Integer.valueOf(prop.getProperty("rabbitmq.port")), 
					prop.getProperty("rabbitmq.virtualHost"), timeout == null ? 0 : Integer.valueOf(timeout), 
					prop.getProperty("rabbitmq.userName"), prop.getProperty("rabbitmq.password"));

	}
	
	private static InputStream readProperties() throws FileNotFoundException {
		logger.debug("加载 rabbitmq配置文件：{}", configFilePath);
		InputStream in = ClassLoader.getSystemResourceAsStream(configFilePath);
		if (in == null) {
			try {
				File file = new File(FilePath.getAbsolutePathWithClass() + configFilePath);
				in = new FileInputStream(file);
			} catch (FileNotFoundException e) {
				logger.error("加载rabbitmq配置文件失败:", e);
			}
		}
		return in;
	}
	
	public static Connection getResource() {
		return pool.getResource();
	}
	
	public static void returnResource(Connection resource) {
		pool.returnResource(resource);
	}
	
	public static Channel getChannel(String queue){
		return getChannel(queue, amqDirect, exchangeType, routeKey);
	}
	
	public synchronized static Channel getChannel(String queue, String amqDirect, String exchangeType, String routeKey){
		Channel channel = null;
		if(amqDirect == null || exchangeType == null){
			return null;
		}
		if(conn == null){
			conn = getResource();
		}
		try {
			channel = conn.createChannel();
			//循环100次后如果未获取到channel，则跳出。
			int i = 100;
			while(channel == null && i-- > 0){
				Connection connMan = getResource();
				conn = getResource();
				channel = conn.createChannel();
				returnResource(connMan);
			}
			if(channel == null){
				//将报空指针异常。
				//throw new RuntimeException("channel create error.");
			}
			channel.exchangeDeclare(amqDirect, exchangeType, true);
	        channel.queueBind(queue, amqDirect, routeKey);
		} catch (IOException e) {
			logger.error("get channel error.", e);
		}
		return channel;
	}
	
	public static void closeChannel(Channel channel){
		try {
			channel.close();
		} catch (IOException e) {
			logger.error("channel close error.", e);
		}
	}
	
	public static void main(String[] args) {
		System.out.println("sdsdfsf");
		for(int i = 0; i < 600; i++){
			Channel channel = RabbitmqConnectionUtil.getChannel("kfpanda");
			closeChannel(channel);
			System.out.println(channel + "----------" + i);
		}
		System.out.println("11111111111111");
	}
}
