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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class MqTest {
	private static Logger logger = LoggerFactory.getLogger(MqTest.class);
	private static final String configFilePath = "/properties/application.properties";
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
	
	public static void main(String[] args) throws IOException, InterruptedException {
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
		
		ConnectionFactory factory = new ConnectionFactory();
		Connection conn;
		String host = prop.getProperty("rabbitmq.host");
        int port = Integer.valueOf(prop.getProperty("rabbitmq.port"));
        String virtualHost = prop.getProperty("rabbitmq.virtualHost");
        int timeout = Integer.valueOf(prop.getProperty("rabbitmq.timeout"));
        String userName = prop.getProperty("rabbitmq.userName");
        String password = prop.getProperty("rabbitmq.password");
        factory.setHost(host);
        factory.setPort(port);
        if(userName != null && "".equals(userName)){
        	factory.setUsername(userName);
        }
        if(password != null && "".equals(password)){
        	factory.setPassword(password);
        }
        factory.setAutomaticRecoveryEnabled(true);
        if(timeout > 0){
        	factory.setConnectionTimeout(timeout);
        }
        if(virtualHost != null && virtualHost.equals("")){
        	factory.setVirtualHost(virtualHost);
        }
        factory.setRequestedChannelMax(20);
        conn = factory.newConnection();
        for(int i = 0; i < 50; i++){
        	System.out.println("create................" + i);
        	Channel channel = conn.createChannel();
        	System.out.println(channel);
        }
        
        System.out.println("end................");
        Thread.sleep(100000);
	}
}
