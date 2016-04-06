/*
 * Copyright 2011-2015 10jqka.com.cn All right reserved. This software is the confidential and proprietary information
 * of 10jqka.com.cn (Confidential Information"). You shall not disclose such Confidential Information and shall use it
 * only in accordance with the terms of the license agreement you entered into with 10jqka.com.cn.
 */
package com.kfpanda.redis;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kfpanda.core.FilePath;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 类RedisUtil.java的实现描述：
 * redis 公共方法类。
 * 配置文件路径为：classpath: /properties/application.properties
 * @author kfpanda 2014-7-14 上午10:55:45
 */
public class RedisUtil {
	private static Logger logger = LoggerFactory.getLogger(RedisUtil.class);
	private static final String configFilePath = "/properties/application.properties";
	private static JedisPool pool = null;
	
	static {
		Properties prop = new Properties();
		InputStream in = null;
		try {
			in = readProperties();
			prop.load(in);
		} catch (IOException e) {
			logger.error("redis配置文件载入失败：", e);
		}finally{
			IOUtils.closeQuietly(in);
		}
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(100);
		poolConfig.setMaxIdle(5);
		poolConfig.setMaxWaitMillis(1000 * 100);
		// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
		poolConfig.setTestOnBorrow(true);
		pool = new JedisPool(poolConfig, prop.getProperty("cache.redis.host"),
				Integer.valueOf(prop.getProperty("cache.redis.port")));

	}
	
	private static InputStream readProperties() throws FileNotFoundException {
		logger.debug("加载 redis配置文件：{}", configFilePath);
		InputStream in = ClassLoader.getSystemResourceAsStream(configFilePath);
		if (in == null) {
			try {
				File file = new File(FilePath.getAbsolutePathWithClass() + configFilePath);
				in = new FileInputStream(file);
			} catch (FileNotFoundException e) {
				logger.error("加载redis配置文件失败:", e);
			}
		}
		return in;
	}

	public static Jedis getResource() {
		return pool.getResource();
	}

	public static void returnResource(Jedis resource) {
		pool.returnResource(resource);
	}

	/*
	 * 释放redis对象。
	 */
	public static void returnBrokenResource(Jedis resource) {
		pool.returnBrokenResource(resource);
	}

	public static void hdelForFields(String key, List<String> fields) {
		if (StringUtils.isNotBlank(key) && fields != null && fields.size() > 0) {
			Jedis jedis = RedisUtil.getResource();
			try {
				for (String field : fields) {
					long result = jedis.hdel(key, field);
					logger.debug("Redis.hdelForFields set: result({}).", result);
				}
			} catch (Exception e) {
				// 释放redis对象
				pool.returnBrokenResource(jedis);
				e.printStackTrace();
			} finally {
				RedisUtil.returnResource(jedis);
			}
		}
	}

	public static void hdel(String key, String field) {
		if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(field)) {
			Jedis jedis = RedisUtil.getResource();
			try {
				long result = jedis.hdel(key, field);
				logger.debug("Redis.hdel set: result({}).", result);
			} catch (Exception e) {
				// 释放redis对象
				pool.returnBrokenResource(jedis);
				e.printStackTrace();
			} finally {
				RedisUtil.returnResource(jedis);
			}
		}
	}

	public static List<String> hvals(String key) {
		List<String> retValue = null;
		if (StringUtils.isNotBlank(key)) {
			Jedis jedis = RedisUtil.getResource();
			try {
				retValue = jedis.hvals(key);
				logger.debug("Redis.hvals : result({}).", retValue);
			} catch (Exception e) {
				// 释放redis对象
				pool.returnBrokenResource(jedis);
				e.printStackTrace();
			} finally {
				RedisUtil.returnResource(jedis);
			}
		}

		return retValue;
	}

	public static Set<String> hkeys(String key) {
		Set<String> retValue = null;
		if (StringUtils.isNotBlank(key)) {
			Jedis jedis = RedisUtil.getResource();
			try {
				retValue = jedis.hkeys(key);
				logger.debug("Redis.hkeys : result({}).", retValue);
			} catch (Exception e) {
				// 释放redis对象
				pool.returnBrokenResource(jedis);
				e.printStackTrace();
			} finally {
				RedisUtil.returnResource(jedis);
			}
		}
		return retValue;
	}

	public static void expire(String key, int seconds) {
		if (StringUtils.isNotBlank(key)) {
			Jedis jedis = RedisUtil.getResource();
			try {
				Long result = jedis.expire(key, seconds);
				logger.debug("Redis.expire result for key: key({}), result({}).",key, result);
			} catch (Exception e) {
				// 释放redis对象
				pool.returnBrokenResource(jedis);
				e.printStackTrace();
			} finally {
				RedisUtil.returnResource(jedis);
			}
		}
	}
}
