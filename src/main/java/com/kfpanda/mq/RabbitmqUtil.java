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
import java.io.Serializable;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kfpanda.core.FilePath;
import com.kfpanda.core.json.JsonUtils;
import com.kfpanda.mq.pool.ChannelPool;
import com.kfpanda.mq.pool.PoolConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * 类RabbitmqUtil.java的实现描述：
 * Rabbitmq 公共方法类。
 * 配置文件路径为：classpath: /properties/application.properties
 * @author kfpanda 2015-4-8 上午10:55:45
 */
public class RabbitmqUtil {
	private static Logger logger = LoggerFactory.getLogger(RabbitmqUtil.class);
	private static final String configFilePath = "/properties/application.properties";
	private static ChannelPool pool = null;
	
	private static final ConnectionFactory factory = new ConnectionFactory();
    private static final String host;
    private static final int port;
    private static final int DEFAULT_PORT = 5672;
    private static final String virtualHost;
    private static final int timeout;
    private static final String userName;
    private static final String password;
    
    private static String queue;
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
		poolConfig.setMaxActive(50);
		poolConfig.setMaxIdle(20);
		poolConfig.setMinIdle(5);
		poolConfig.setMaxWait(1000 * 100);
		// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
		poolConfig.setTestOnBorrow(true);
		String tmout = prop.getProperty("rabbitmq.timeout");
		queue = prop.getProperty("rabbitmq.queue");
		amqDirect = prop.getProperty("rabbitmq.amq.direct");
		exchangeType = prop.getProperty("rabbitmq.exchange.type");
		routeKey = prop.getProperty("rabbitmq.route.key");
		
		host = prop.getProperty("rabbitmq.host");
		port = prop.getProperty("rabbitmq.port") == null 
				? DEFAULT_PORT : Integer.valueOf(prop.getProperty("rabbitmq.port"));
		virtualHost = prop.getProperty("rabbitmq.virtualHost");
		timeout = tmout == null ? 0 : Integer.valueOf(tmout);
		userName = prop.getProperty("rabbitmq.userName");
		password = prop.getProperty("rabbitmq.password");

		factory.setHost(host);
        factory.setPort(port);
        if(userName != null && userName != ""){
        	factory.setUsername(userName);
        }
        if(password != null && password != ""){
        	factory.setPassword(password);
        }
        factory.setAutomaticRecoveryEnabled(true);
//        factory.setRequestedChannelMax(50);
        if(timeout > 0){
        	factory.setConnectionTimeout(timeout);
        }
        if(virtualHost != null && virtualHost != ""){
        	factory.setVirtualHost(virtualHost);
        }
        
        Connection conn = null;
		try {
			conn = factory.newConnection();
		} catch (IOException e) {
			logger.error("rabbitmq factory new connection error.", e);
		}
        
		pool = new ChannelPool(poolConfig, conn, queue, amqDirect, exchangeType, routeKey);

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
	
	public static Channel getResource() {
		return pool.getResource();
	}
	
	public static void returnResource(Channel resource) {
		pool.returnResource(resource);
	}
	
	public void publish(final Serializable obj){
		Channel channel = getResource();
		String content = JsonUtils.toJsonString(obj);
		try {
			channel.basicPublish(exchangeType, routeKey, null, content.getBytes("UTF-8"));
		} catch (IOException e) {
			logger.error("消息（{}）发送失败.", content);
			logger.error("", e);
		}finally{
			returnResource(channel);
		}
	}
	
	/*public static Channel getChannel(String queue){
		return getChannel(queue, amqDirect, exchangeType, routeKey);
	}
	
	public static Channel getChannel(String queue, String amqDirect, String exchangeType, String routeKey){
		Channel channel = getResource();
		try {
			channel.exchangeDeclare(amqDirect, exchangeType, true);
	        channel.queueBind(queue, amqDirect, routeKey);
		} catch (IOException e) {
			logger.error("get channel error.", e);
		}
		return channel;
	}*/
	
	public static void closeChannel(Channel channel){
		try {
			channel.close();
		} catch (IOException e) {
			logger.error("channel close error.", e);
		}
	}
	
	public static void main(String[] args) {
		System.out.println("sdsdfsf");
		for(int i = 0; i < 20; i++){
			Channel channel = RabbitmqUtil.getResource();
			try {
				channel.basicPublish(exchangeType, routeKey, null, "test----".getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			RabbitmqUtil.returnResource(channel);
//			closeChannel(channel);
			System.out.println(channel + "----------" + i);
		}
		System.out.println("11111111111111");
	}
}
